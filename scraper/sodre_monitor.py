#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sodré Santoro Monitor - Detecção de Lances (CORRIGIDO)
✅ Usa mesma lógica do scraper (só leilões ativos)
✅ Carrega itens do banco E captura dados atuais da API
✅ Cruza os dois para detectar mudanças
"""

import asyncio
import os
import sys
from datetime import datetime
from playwright.async_api import async_playwright
from supabase import create_client, Client

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ✅ URLs IGUAIS AO SCRAPER (só leilões ativos, ordenados por data)
SODRE_URLS = [
    "https://www.sodresantoro.com.br/veiculos/lotes?sort=auction_date_init_asc",
    "https://www.sodresantoro.com.br/imoveis/lotes?sort=auction_date_init_asc",
    "https://www.sodresantoro.com.br/materiais/lotes?sort=auction_date_init_asc",
    "https://www.sodresantoro.com.br/sucatas/lotes?sort=auction_date_init_asc",
]

HOT_ITEM_THRESHOLD_VALUE = 1000
HOT_ITEM_THRESHOLD_PERCENT = 20


class SodreMonitor:
    """Monitor de lances Sodré Santoro com detecção de padrões"""
    
    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("❌ SUPABASE_URL e SUPABASE_KEY devem estar definidas")
        
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.db_items = {}
        self.api_lots = {}
    
    def load_database_items(self):
        """Carrega TODOS os itens Sodré Santoro ativos do banco"""
        print("📥 Carregando itens do banco (Sodré Santoro ativos)...")
        
        try:
            page_size = 1000
            offset = 0
            total_loaded = 0
            
            while True:
                response = self.supabase.schema("auctions").table("vw_auctions_unified")\
                    .select("link,category,source,external_id,lot_number,total_bids,total_bidders,value")\
                    .eq("source", "sodre")\
                    .eq("is_active", True)\
                    .range(offset, offset + page_size - 1)\
                    .execute()
                
                if not response.data:
                    break
                
                for item in response.data:
                    link = item.get("link")
                    if link:
                        self.db_items[link] = {
                            "category": item.get("category"),
                            "source": item.get("source"),
                            "external_id": item.get("external_id"),
                            "lot_number": item.get("lot_number"),
                            "prev_bid": float(item.get("value") or 0),
                            "prev_bids": int(item.get("total_bids") or 0),
                            "prev_bidders": int(item.get("total_bidders") or 0),
                        }
                
                total_loaded += len(response.data)
                print(f"   → Carregados {total_loaded} itens...")
                
                if len(response.data) < page_size:
                    break
                
                offset += page_size
            
            print(f"✅ {len(self.db_items)} itens Sodré carregados da view")
            
            if self.db_items:
                print(f"\n📋 Exemplos de links no banco:")
                for i, link in enumerate(list(self.db_items.keys())[:3]):
                    print(f"   {i+1}. {link}")
                print()
            
            return True
            
        except Exception as e:
            print(f"❌ Erro ao carregar itens: {e}\n")
            return False
    
    async def intercept_sodre_data(self):
        """
        Intercepta dados da API Sodré usando Playwright
        ✅ IGUAL AO SCRAPER: Só páginas ativas, para quando não há mais botão
        """
        print("🌐 Iniciando interceptação Playwright...\n")
        
        all_lots = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='pt-BR'
            )
            
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            page = await context.new_page()
            
            async def intercept_response(response):
                try:
                    if '/api/search-lots' in response.url and response.status == 200:
                        data = await response.json()
                        per_page = data.get('perPage', 0)
                        
                        if per_page > 0:
                            results = data.get('results', [])
                            hits = data.get('hits', {}).get('hits', [])
                            
                            if results:
                                all_lots.extend(results)
                            elif hits:
                                extracted = [hit.get('_source', hit) for hit in hits]
                                all_lots.extend(extracted)
                
                except Exception:
                    pass
            
            page.on('response', intercept_response)
            
            # ✅ NAVEGA IGUAL AO SCRAPER
            for url in SODRE_URLS:
                section_name = url.split('/')[3]
                print(f"📦 {section_name.upper()}")
                print(f"   🌐 {url}")
                
                lots_before = len(all_lots)
                
                try:
                    await page.goto(url, wait_until="networkidle", timeout=60000)
                    await asyncio.sleep(3)
                    
                    # ✅ PAGINAÇÃO IGUAL AO SCRAPER (para quando botão disabled)
                    for page_num in range(2, 51):
                        try:
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await asyncio.sleep(2)
                            
                            selectors = [
                                'button[title="Avançar"]:not([disabled])',
                                'button[title*="Avanç"]:not([disabled])',
                                'button:has(.i-mdi\\:chevron-right):not([disabled])',
                            ]
                            
                            clicked = False
                            for selector in selectors:
                                try:
                                    button = page.locator(selector).first
                                    if await button.count() > 0:
                                        is_disabled = await button.get_attribute('disabled')
                                        if is_disabled is None:
                                            await button.click()
                                            print(f"   ➡️  Página {page_num}...")
                                            await asyncio.sleep(4)
                                            clicked = True
                                            break
                                except:
                                    continue
                            
                            if not clicked:
                                print(f"   ✅ {page_num-1} páginas processadas")
                                break
                        
                        except Exception as e:
                            break
                
                except Exception as e:
                    print(f"   ⚠️ Erro ao carregar URL: {e}")
                
                lots_section = len(all_lots) - lots_before
                print(f"   📊 {lots_section} lotes desta seção\n")
            
            await browser.close()
        
        print(f"📊 {len(all_lots)} lotes capturados da API")
        print(f"🔍 Indexando por link...\n")
        
        # Indexa por link
        for lot in all_lots:
            auction_id = lot.get('auction_id')
            lot_id = lot.get('lot_id')
            
            if auction_id and lot_id:
                link = f"https://leilao.sodresantoro.com.br/leilao/{auction_id}/lote/{lot_id}/"
                self.api_lots[link] = lot
        
        print(f"✅ {len(self.api_lots)} lotes únicos indexados\n")
        return len(self.api_lots) > 0
    
    def cross_reference_data(self):
        """Cruza dados do banco com dados da API"""
        print("🔗 Cruzando dados (DB ↔ API)...\n")
        
        matched_records = []
        hot_items = []
        
        for link, db_data in self.db_items.items():
            api_data = self.api_lots.get(link)
            
            if not api_data:
                continue
            
            # Extrai dados da API Sodré
            current_value = float(api_data.get('bid_actual') or 0)
            has_bid = api_data.get('bid_has_bid', False)
            
            # Contadores de lances
            total_bids = int(api_data.get('bid_count') or api_data.get('total_bids') or 0)
            total_bidders = int(api_data.get('bidder_count') or api_data.get('total_bidders') or 0)
            
            # Calcula variações
            prev_value = db_data['prev_bid']
            value_delta = current_value - prev_value
            value_increase_pct = (value_delta / prev_value * 100) if prev_value > 0 else 0
            
            bid_delta = total_bids - db_data['prev_bids']
            
            # Prepara registro
            record = {
                "category": db_data["category"],
                "source": db_data["source"],
                "external_id": db_data["external_id"],
                "lot_number": db_data["lot_number"],
                # Campos para auction_bid_history
                "total_bids": total_bids,
                "total_bidders": total_bidders,
                "current_value": current_value,
                "captured_at": datetime.now().isoformat(),
                # Metadados para análise
                "_value_delta": value_delta,
                "_value_increase_pct": value_increase_pct,
                "_bid_delta": bid_delta,
                "_has_bid": has_bid,
            }
            
            matched_records.append(record)
            
            # Detecta itens quentes
            is_hot = (
                value_delta >= HOT_ITEM_THRESHOLD_VALUE or 
                value_increase_pct >= HOT_ITEM_THRESHOLD_PERCENT or
                bid_delta >= 5
            )
            
            if is_hot:
                hot_items.append({
                    **record,
                    "lot_title": f"{api_data.get('lot_brand', '')} {api_data.get('lot_model', '')}".strip(),
                })
        
        print(f"✅ {len(matched_records)} matches encontrados\n")
        
        if hot_items:
            print(f"{'='*70}")
            print(f"🔥 {len(hot_items)} ITENS QUENTES DETECTADOS!")
            print(f"{'='*70}\n")
            
            hot_items.sort(key=lambda x: x.get('_value_increase_pct', 0), reverse=True)
            
            for i, item in enumerate(hot_items[:10], 1):
                print(f"{i:2d}. 🚨 Lote {item['lot_number']}: {item['lot_title']}")
                print(f"      Valor: R$ {item['current_value']:,.2f} "
                      f"(+R$ {item['_value_delta']:,.2f} / +{item['_value_increase_pct']:.1f}%)")
                if item['_bid_delta'] > 0:
                    print(f"      Lances: {item['total_bids']} (+{item['_bid_delta']})")
                print()
        
        return matched_records, hot_items
    
    def update_base_tables(self, records):
        """Atualiza tabelas base com os campos corretos"""
        if not records:
            return 0
        
        updated_count = 0
        
        by_category = {}
        for record in records:
            cat = record["category"]
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(record)
        
        print("🔄 Atualizando tabelas base...\n")
        
        for category, cat_records in by_category.items():
            cat_updated = 0
            cat_errors = 0
            
            for record in cat_records:
                try:
                    self.supabase.schema("auctions").table(category)\
                        .update({
                            "value": record["current_value"],
                            "total_bids": record["total_bids"],
                            "total_bidders": record["total_bidders"],
                            "last_scraped_at": record["captured_at"]
                        })\
                        .eq("source", record["source"])\
                        .eq("external_id", record["external_id"])\
                        .execute()
                    
                    cat_updated += 1
                    updated_count += 1
                    
                except Exception as e:
                    cat_errors += 1
                    if cat_errors <= 3:
                        print(f"   ⚠️  Erro em {category}: {e}")
                    continue
            
            if cat_updated > 0:
                print(f"   ✅ {category:45s} | {cat_updated:3d} atualizados")
            elif cat_errors > 0:
                print(f"   ❌ {category:45s} | {cat_errors:2d} erros")
        
        print()
        return updated_count
    
    def save_bid_history(self, records):
        """Salva histórico com os campos corretos"""
        if not records:
            return 0
        
        try:
            # Remove metadados internos (_*)
            clean_records = []
            for record in records:
                clean = {
                    "category": record["category"],
                    "source": record["source"],
                    "external_id": record["external_id"],
                    "lot_number": record["lot_number"],
                    "total_bids": record["total_bids"],
                    "total_bidders": record["total_bidders"],
                    "current_value": record["current_value"],
                    "captured_at": record["captured_at"]
                }
                clean_records.append(clean)
            
            # Remove duplicatas
            unique_records = {}
            for record in clean_records:
                key = (
                    record["category"],
                    record["source"],
                    record["external_id"],
                    record["captured_at"][:19]
                )
                unique_records[key] = record
            
            records_to_insert = list(unique_records.values())
            
            # Upsert
            response = self.supabase.schema("auctions").table("auction_bid_history")\
                .upsert(records_to_insert, on_conflict="category,source,external_id,captured_at")\
                .execute()
            
            print(f"💾 {len(response.data)} registros salvos no histórico\n")
            return len(response.data)
            
        except Exception as e:
            print(f"❌ Erro ao salvar histórico: {e}\n")
            return 0
    
    async def run(self):
        """Executa monitoramento completo"""
        print("\n" + "="*70)
        print("🔵 SODRÉ SANTORO MONITOR - DETECÇÃO DE LANCES")
        print("="*70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        print()
        
        if not self.load_database_items():
            return False
        
        if not self.db_items:
            print("⚠️ Nenhum item ativo no banco\n")
            return True
        
        if not await self.intercept_sodre_data():
            print("❌ Falha ao capturar dados da API\n")
            return False
        
        matched_records, hot_items = self.cross_reference_data()
        
        if not matched_records:
            print("⚠️ Nenhum match encontrado\n")
            return True
        
        updated = self.update_base_tables(matched_records)
        saved = self.save_bid_history(matched_records)
        
        print("="*70)
        print("📊 RESUMO DA EXECUÇÃO")
        print("="*70)
        print(f"📋 Itens no banco:        {len(self.db_items)}")
        print(f"🔵 Lotes da API:          {len(self.api_lots)}")
        print(f"🔗 Matches:               {len(matched_records)}")
        print(f"🔄 Tabelas atualizadas:   {updated}")
        print(f"💾 Histórico salvo:       {saved}")
        print(f"🔥 Itens quentes:         {len(hot_items)}")
        print("="*70)
        
        match_rate = (len(matched_records) / len(self.db_items) * 100) if self.db_items else 0
        print(f"\n📈 Taxa de match: {match_rate:.1f}%")
        
        if match_rate < 50:
            print("\n⚠️ Taxa de match baixa! Possíveis causas:")
            print("   • Links no banco podem estar em formato diferente")
            print("   • Muitos lotes já finalizaram")
            print("   • Paginação não capturou todas as páginas")
        
        print()
        return True


async def main():
    try:
        monitor = SodreMonitor()
        success = await monitor.run()
        
        if success:
            print("✅ Monitor executado com sucesso!\n")
            sys.exit(0)
        else:
            print("❌ Monitor falhou\n")
            sys.exit(1)
    
    except Exception as e:
        print(f"\n❌ ERRO FATAL: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())