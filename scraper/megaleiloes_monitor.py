#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MegaLeilões Monitor - Histórico de Lances (VERSÃO CORRIGIDA)

MUDANÇAS:
✅ Extrai has_bid CORRETAMENTE (número > 0) usando 3 estratégias
✅ Salva apenas has_bid + current_value no histórico
✅ Atualiza tabelas base com has_bid + value

FUNCIONAMENTO:
1. Carrega TODOS os itens MegaLeilões ativos da view vw_auctions_unified
2. Scraping via Playwright nas 6 categorias
3. Compara links (normaliza UTM params)
4. Para cada match: salva snapshot no auction_bid_history
5. Atualiza tabela base com has_bid + value
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

# Categorias MegaLeilões
MEGA_CATEGORIES = [
    'https://www.megaleiloes.com.br/veiculos',
    'https://www.megaleiloes.com.br/imoveis',
    'https://www.megaleiloes.com.br/bens-de-consumo',
    'https://www.megaleiloes.com.br/industrial',
    'https://www.megaleiloes.com.br/animais',
    'https://www.megaleiloes.com.br/outros',
]


class MegaLeiloesMonitor:
    """Monitor de lances MegaLeilões (VERSÃO CORRIGIDA)"""
    
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
        print("🔥 Carregando itens do banco (MegaLeilões ativos)...")
        
        try:
            page_size = 1000
            offset = 0
            total_loaded = 0
            
            while True:
                response = self.supabase.schema("auctions").table("vw_auctions_unified")\
                    .select("link,category,source,external_id,lot_number")\
                    .eq("source", "megaleiloes")\
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
            
            print(f"✅ {len(self.db_items)} itens MegaLeilões carregados da view")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao carregar itens: {e}")
            return False
    
    def extract_has_bid_robust(self, card) -> bool:
        """
        ✅ VERSÃO CORRIGIDA: Extrai has_bid com 3 estratégias (adaptado para Playwright)
        
        HTML esperado:
        <div class="card-views-bids">
            <span><i class="fa fa-eye"></i> 1592</span>
            <span><i class="fa fa-legal"></i> 0</span>     <!-- Lances -->
        </div>
        
        Retorna:
            True se número de lances > 0
            False caso contrário
        """
        try:
            # Estratégia 1: Buscar o span que contém o ícone fa-legal
            legal_span = card.query_selector('span:has(i.fa-legal)')
            if legal_span:
                text = legal_span.inner_text().strip()
                numbers = re.findall(r'\d+', text)
                if numbers:
                    bid_count = int(numbers[0])
                    return bid_count > 0
            
            # Estratégia 2: Buscar ícone e pegar texto do parent
            legal_icon = card.query_selector('i.fa-legal')
            if legal_icon:
                # Em Playwright, precisamos avaluar JS para pegar o parent
                parent_text = card.evaluate('''(card) => {
                    const icon = card.querySelector('i.fa-legal');
                    if (icon && icon.parentElement) {
                        return icon.parentElement.textContent;
                    }
                    return null;
                }''')
                
                if parent_text:
                    numbers = re.findall(r'\d+', parent_text)
                    if numbers:
                        bid_count = int(numbers[0])
                        return bid_count > 0
            
            # Estratégia 3: Buscar container card-views-bids
            views_bids = card.query_selector('.card-views-bids, div[class*="views-bids"]')
            if views_bids:
                spans = views_bids.query_selector_all('span')
                for span in spans:
                    # Verifica se tem o ícone fa-legal
                    if span.query_selector('i.fa-legal'):
                        text = span.inner_text().strip()
                        numbers = re.findall(r'\d+', text)
                        if numbers:
                            bid_count = int(numbers[0])
                            return bid_count > 0
            
            return False
            
        except Exception as e:
            # Em caso de erro, retorna False (sem lance)
            return False
    
    def extract_card_data(self, card):
        """
        Extrai dados de um card HTML (VERSÃO CORRIGIDA)
        
        Retorna:
            {
                "link": str,
                "external_id": str,
                "current_value": float,
                "has_bid": bool  # ✅ TRUE apenas se número de lances > 0
            }
        """
        try:
            # Link
            link_elem = card.query_selector('a.card-title')
            if not link_elem:
                return None
            link = link_elem.get_attribute('href')
            if not link:
                return None
            
            # External ID (card-number)
            external_id_elem = card.query_selector('.card-number')
            external_id = external_id_elem.inner_text().strip() if external_id_elem else None
            
            # Valor atual
            price_elem = card.query_selector('.card-price')
            price_text = price_elem.inner_text().strip() if price_elem else "R$ 0"
            current_value = float(re.sub(r'[^\d,]', '', price_text).replace(',', '.')) if price_text else 0
            
            # ✅ has_bid: EXTRAÇÃO ROBUSTA (número > 0)
            has_bid = self.extract_has_bid_robust(card)
            
            return {
                "link": link,
                "external_id": external_id,
                "current_value": current_value,
                "has_bid": has_bid,  # ✅ Boolean correto
            }
            
        except Exception as e:
            print(f"⚠️ Erro ao extrair card: {e}")
            return None
    
    def scrape_category(self, page, category_url: str):
        """Scraping de uma categoria com paginação"""
        cards_data = []
        current_page = 1
        category_name = category_url.split('/')[-1]
        
        try:
            while True:
                page_url = f"{category_url}?pagina={current_page}" if current_page > 1 else category_url
                
                page.goto(page_url, wait_until="networkidle", timeout=60000)
                page.wait_for_timeout(2000)
                
                # Scroll para carregar lazy load
                for _ in range(3):
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(1000)
                
                # Extrai cards da página atual
                cards = page.query_selector_all('.card-content')
                
                page_cards = 0
                for card in cards:
                    card_data = self.extract_card_data(card)
                    if card_data:
                        cards_data.append(card_data)
                        page_cards += 1
                
                # Verifica próxima página
                next_btn = page.query_selector('.pagination .next:not(.disabled) a')
                if not next_btn:
                    break
                
                current_page += 1
            
        except Exception as e:
            print(f"❌ Erro em {category_url}: {e}")
        
        return cards_data, category_name, current_page
    
    def process_scraped_data(self, scraped_items):
        """
        Processa dados scraped e faz match com banco
        
        Retorna lista de registros para histórico:
        [
            {
                "category": str,
                "source": str,
                "external_id": str,
                "lot_number": str,
                "has_bid": bool,  # ✅ Boolean correto
                "current_value": float,
                "captured_at": str (ISO)
            }
        ]
        """
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
                "has_bid": item["has_bid"],  # ✅ Boolean correto
                "current_value": item["current_value"],
                "captured_at": datetime.now().isoformat(),
            }
            
            all_records.append(record)
        
        return all_records
    
    def update_base_tables(self, records):
        """
        Atualiza tabelas base com has_bid + value
        
        ✅ MUDANÇA: Agora atualiza has_bid (boolean correto) ao invés de total_bids
        """
        if not records:
            return 0
        
        updated_count = 0
        errors = 0
        
        # Agrupa por categoria para logs organizados
        by_category = {}
        for record in records:
            cat = record["category"]
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(record)
        
        for category, cat_records in by_category.items():
            cat_updated = 0
            cat_errors = 0
            
            for record in cat_records:
                try:
                    self.supabase.schema("auctions").table(category)\
                        .update({
                            "has_bid": record["has_bid"],  # ✅ Boolean correto
                            "value": record["current_value"],
                            "last_scraped_at": record["captured_at"]
                        })\
                        .eq("source", record["source"])\
                        .eq("external_id", record["external_id"])\
                        .execute()
                    
                    cat_updated += 1
                    updated_count += 1
                    
                except Exception as e:
                    cat_errors += 1
                    errors += 1
                    continue
            
            # Log por categoria
            if cat_updated > 0:
                print(f"✅ {category:25s} | {cat_updated:3d} atualizados | {cat_errors:2d} erros")
            elif cat_errors > 0:
                print(f"❌ {category:25s} | 0 atualizados | {cat_errors:2d} erros")
        
        return updated_count
    
    def save_bid_history(self, records):
        """
        Salva histórico de lances em lote
        
        ✅ MUDANÇA: Agora salva has_bid (boolean correto) ao invés de total_bids
        """
        if not records:
            return 0
        
        try:
            # Remove duplicatas por chave única
            unique_records = {}
            for record in records:
                key = (
                    record["category"],
                    record["source"],
                    record["external_id"],
                    record["captured_at"][:19]  # Trunca para segundos
                )
                unique_records[key] = record
            
            records_to_insert = list(unique_records.values())
            
            # Upsert no histórico
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
        print("🔵 MEGALEILÕES MONITOR - HISTÓRICO DE LANCES (VERSÃO CORRIGIDA)")
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
            
            for category_url in MEGA_CATEGORIES:
                cards, cat_name, pages = self.scrape_category(page, category_url)
                all_scraped.extend(cards)
                
                # Calcula matches desta categoria
                cat_matches = 0
                cat_with_bid = 0
                for item in cards:
                    link = self.normalize_link(item["link"])
                    if link in self.db_items:
                        cat_matches += 1
                        if item.get("has_bid"):
                            cat_with_bid += 1
                
                category_stats.append({
                    "name": cat_name,
                    "scraped": len(cards),
                    "matches": cat_matches,
                    "with_bid": cat_with_bid,
                    "pages": pages
                })
            
            browser.close()
        
        # Exibe stats por categoria
        for stat in category_stats:
            name = stat["name"]
            scraped = stat["scraped"]
            matches = stat["matches"]
            with_bid = stat["with_bid"]
            pages = stat["pages"]
            
            if matches > 0:
                print(f"✅ {name:25s} | {scraped:3d} scraped | {matches:3d} matches | {with_bid:3d} c/ lance | {pages} pág(s)")
            else:
                print(f"⚪ {name:25s} | {scraped:3d} scraped | 0 matches | 0 c/ lance | {pages} pág(s)")
        
        # Processa e salva
        all_records = self.process_scraped_data(all_scraped)
        matched_count = len(all_records)
        
        # Conta quantos têm lance
        with_bid_count = sum(1 for r in all_records if r.get("has_bid"))
        
        print("\n" + "="*70)
        print("🔄 Atualizando tabelas base (has_bid, value, last_scraped_at)...")
        print("="*70)
        print()
        
        updated = self.update_base_tables(all_records)
        
        print()
        print("="*70)
        print("💾 Salvando histórico de lances na tabela auction_bid_history...")
        print("="*70)
        
        saved = self.save_bid_history(all_records)
        
        print(f"\n✅ {saved} registros salvos no histórico")
        
        print("\n" + "="*70)
        print("📊 RESUMO DA EXECUÇÃO")
        print("="*70)
        print(f"📋 Itens MegaLeilões na view: {len(self.db_items)}")
        print(f"🌐 Ofertas scraped: {len(all_scraped)}")
        print(f"🔗 Links matched (encontrados): {matched_count}")
        print(f"🔥 Itens com lance (has_bid=TRUE): {with_bid_count}")
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
        monitor = MegaLeiloesMonitor()
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