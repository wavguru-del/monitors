#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sodré Santoro Monitor - Histórico de Lances

FUNCIONAMENTO:
1. Carrega TODOS os itens Sodré Santoro ativos da view vw_auctions_unified
2. Scraping via Playwright nas 4 categorias
3. Compara links (normaliza UTM params)
4. Para cada match: atualiza tabelas base + salva histórico na auction_bid_history
"""

import os
import sys
import re
from datetime import datetime
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
from supabase import create_client, Client

# Configuração
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Categorias Sodré Santoro
SODRE_CATEGORIES = [
    'https://www.sodresantoro.com.br/veiculos/lotes?sort=auction_date_init_asc',
    'https://www.sodresantoro.com.br/materiais/lotes?sort=auction_date_init_asc',
    'https://www.sodresantoro.com.br/imoveis/lotes?sort=auction_date_init_asc',
    'https://www.sodresantoro.com.br/sucatas/lotes?sort=auction_date_init_asc',
]


class SodreSantoroMonitor:
    """Monitor de lances Sodré Santoro"""
    
    def __init__(self):
        """Inicializa conexões"""
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidas")
        
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.db_items = {}  # {link_normalizado: {category, source, external_id, lot_number}}
    
    @staticmethod
    def normalize_link(link: str) -> str:
        """Remove UTM params e normaliza link"""
        if not link:
            return ""
        parsed = urlparse(link)
        # Remove query params
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
    
    def load_database_items(self):
        """Carrega TODOS os itens ativos do banco indexados por link"""
        print("📥 Carregando itens do banco (Sodré Santoro ativos)...")
        
        try:
            page_size = 1000
            offset = 0
            total_loaded = 0
            
            while True:
                response = self.supabase.schema("auctions").table("vw_auctions_unified")\
                    .select("link,category,source,external_id,lot_number")\
                    .eq("source", "sodre")\
                    .eq("is_active", True)\
                    .range(offset, offset + page_size - 1)\
                    .execute()
                
                if not response.data:
                    break
                
                for item in response.data:
                    link = item.get("link")
                    if link:
                        normalized = self.normalize_link(link)
                        self.db_items[normalized] = {
                            "category": item.get("category"),
                            "source": item.get("source"),
                            "external_id": item.get("external_id"),
                            "lot_number": item.get("lot_number"),
                        }
                
                total_loaded += len(response.data)
                print(f"   → Carregados {total_loaded} itens...")
                
                if len(response.data) < page_size:
                    break
                
                offset += page_size
            
            print(f"✅ {len(self.db_items)} itens Sodré Santoro carregados da view")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao carregar itens: {e}")
            return False
    
    def extract_lot_data(self, lot_card):
        """Extrai dados de um card de lote Sodré Santoro"""
        try:
            # Link do lote
            link_elem = lot_card.query_selector('a[href*="/lote/"]')
            if not link_elem:
                return None
            
            link = link_elem.get_attribute('href')
            if not link:
                return None
            
            # Normaliza link (adiciona domínio se necessário)
            if link.startswith('/'):
                link = f"https://www.sodresantoro.com.br{link}"
            
            # Número do lote (geralmente visível no card)
            lot_number_elem = lot_card.query_selector('.lot-number, .lote-numero, [class*="lot"]')
            lot_number = None
            if lot_number_elem:
                lot_text = lot_number_elem.inner_text().strip()
                match = re.search(r'Lote\s*:?\s*(\d+)', lot_text, re.IGNORECASE)
                if match:
                    lot_number = match.group(1)
            
            # Valor atual
            price_elem = lot_card.query_selector('.price, .valor, [class*="price"], [class*="valor"]')
            current_value = 0
            if price_elem:
                price_text = price_elem.inner_text().strip()
                # Remove tudo exceto dígitos e vírgula
                price_clean = re.sub(r'[^\d,]', '', price_text)
                if price_clean:
                    current_value = float(price_clean.replace(',', '.'))
            
            # Lances (procura por "X lances" ou similar)
            bids_elem = lot_card.query_selector('[class*="bid"], [class*="lance"]')
            total_bids = 0
            if bids_elem:
                bids_text = bids_elem.inner_text().strip()
                match = re.search(r'(\d+)\s*lances?', bids_text, re.IGNORECASE)
                if match:
                    total_bids = int(match.group(1))
            
            return {
                "link": link,
                "lot_number": lot_number,
                "current_value": current_value,
                "total_bids": total_bids,
            }
            
        except Exception as e:
            print(f"⚠️ Erro ao extrair lote: {e}")
            return None
    
    def scrape_category(self, page, category_url: str):
        """Scraping de uma categoria com scroll infinito"""
        lots_data = []
        category_name = category_url.split('/')[3]  # extrai 'veiculos', 'materiais', etc
        
        try:
            print(f"   → Acessando {category_name}...")
            page.goto(category_url, wait_until="networkidle", timeout=60000)
            page.wait_for_timeout(3000)
            
            # Scroll para carregar mais lotes (scroll infinito)
            prev_count = 0
            no_change_count = 0
            
            for scroll_attempt in range(15):  # Máximo 15 scrolls
                # Scroll até o final
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(2000)
                
                # Conta lotes atuais
                current_lots = page.query_selector_all('.lot-card, .lote-card, [class*="lot-"], article')
                current_count = len(current_lots)
                
                if current_count == prev_count:
                    no_change_count += 1
                    if no_change_count >= 3:  # 3 scrolls sem mudança = fim
                        break
                else:
                    no_change_count = 0
                
                prev_count = current_count
            
            # Extrai todos os lotes
            lot_cards = page.query_selector_all('.lot-card, .lote-card, [class*="lot-"], article')
            
            print(f"   → Encontrados {len(lot_cards)} cards em {category_name}")
            
            for card in lot_cards:
                lot_data = self.extract_lot_data(card)
                if lot_data:
                    lots_data.append(lot_data)
            
        except Exception as e:
            print(f"❌ Erro em {category_url}: {e}")
        
        return lots_data, category_name
    
    def process_scraped_data(self, scraped_items):
        """Processa dados scraped e faz match com banco"""
        all_records = []
        
        for item in scraped_items:
            link = self.normalize_link(item["link"])
            
            db_item = self.db_items.get(link)
            if not db_item:
                continue
            
            record = {
                "category": db_item["category"],
                "source": db_item["source"],
                "external_id": db_item["external_id"],
                "lot_number": db_item["lot_number"],
                "total_bids": item["total_bids"],
                "total_bidders": 0,  # Sodré não fornece no card
                "current_value": item["current_value"],
                "captured_at": datetime.now().isoformat(),
            }
            
            all_records.append(record)
        
        return all_records
    
    def update_base_tables(self, records):
        """Atualiza tabelas base com dados de lances"""
        updated_count = 0
        
        for record in records:
            try:
                table_name = record["category"]
                
                self.supabase.schema("auctions").table(table_name)\
                    .update({
                        "total_bids": record["total_bids"],
                        "total_bidders": record["total_bidders"],
                        "value": record["current_value"],
                        "last_scraped_at": record["captured_at"]
                    })\
                    .eq("source", record["source"])\
                    .eq("external_id", record["external_id"])\
                    .execute()
                
                updated_count += 1
                
            except Exception as e:
                print(f"⚠️ Erro ao atualizar {record['category']}/{record['external_id']}: {e}")
                continue
        
        return updated_count
    
    def save_bid_history(self, records):
        """Salva histórico de lances em lote"""
        if not records:
            return 0
        
        try:
            unique_records = {}
            for record in records:
                key = (
                    record["category"],
                    record["source"],
                    record["external_id"],
                    record["captured_at"][:19]
                )
                unique_records[key] = record
            
            records_to_insert = list(unique_records.values())
            
            response = self.supabase.schema("auctions").table("auction_bid_history")\
                .upsert(records_to_insert, on_conflict="category,source,external_id,captured_at")\
                .execute()
            
            return len(response.data)
            
        except Exception as e:
            print(f"❌ Erro ao salvar histórico: {e}")
            return 0
    
    def run(self):
        """Executa monitoramento completo"""
        print("\n" + "="*70)
        print("🔵 SODRÉ SANTORO MONITOR - HISTÓRICO DE LANCES")
        print("="*70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        if not self.load_database_items():
            print("❌ Falha ao carregar itens do banco")
            return False
        
        if not self.db_items:
            print("⚠️ Nenhum item ativo encontrado no banco")
            return True
        
        # Scraping com Playwright
        all_scraped = []
        category_stats = []
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            print("\n🌐 Buscando ofertas via scraping e comparando links...\n")
            
            for category_url in SODRE_CATEGORIES:
                lots, cat_name = self.scrape_category(page, category_url)
                all_scraped.extend(lots)
                
                # Calcula matches desta categoria
                cat_matches = 0
                for item in lots:
                    link = self.normalize_link(item["link"])
                    if link in self.db_items:
                        cat_matches += 1
                
                category_stats.append({
                    "name": cat_name,
                    "scraped": len(lots),
                    "matches": cat_matches
                })
            
            browser.close()
        
        # Exibe stats por categoria
        for stat in category_stats:
            name = stat["name"]
            scraped = stat["scraped"]
            matches = stat["matches"]
            
            if matches > 0:
                print(f"✅ {name:25s} | {scraped:3d} scraped | {matches:3d} matches")
            else:
                print(f"⚪ {name:25s} | {scraped:3d} scraped | 0 matches")
        
        # Processa e salva
        all_records = self.process_scraped_data(all_scraped)
        matched_count = len(all_records)
        
        print("\n" + "="*70)
        print("🔄 Atualizando tabelas base (total_bids, value, last_scraped_at)...")
        
        updated = self.update_base_tables(all_records)
        
        print("\n💾 Salvando histórico de lances na tabela auction_bid_history...")
        
        saved = self.save_bid_history(all_records)
        
        print("\n" + "="*70)
        print("📊 RESUMO DA EXECUÇÃO")
        print("="*70)
        print(f"📋 Itens Sodré Santoro na view: {len(self.db_items)}")
        print(f"🌐 Ofertas scraped: {len(all_scraped)}")
        print(f"🔗 Links matched (encontrados): {matched_count}")
        print(f"🔄 Tabelas base atualizadas: {updated}")
        print(f"💾 Registros salvos no histórico: {saved}")
        print("="*70)
        
        if len(self.db_items) > 0:
            print(f"\n📈 Taxa de match: {(matched_count/len(self.db_items)*100):.1f}%")
        
        if matched_count < len(self.db_items) * 0.1:
            print(f"⚠️ Poucos matches! Verifique se:")
            print(f"   - Os links no banco estão no formato correto")
            print(f"   - As ofertas ainda estão ativas no site")
        
        return True


def main():
    """Execução principal"""
    try:
        monitor = SodreSantoroMonitor()
        success = monitor.run()
        
        if success:
            print("\n✅ Monitor executado com sucesso!")
            sys.exit(0)
        else:
            print("\n❌ Monitor falhou")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()