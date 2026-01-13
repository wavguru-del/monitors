#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SuperBid Monitor - Histórico de Lances (COM PAGINAÇÃO COMPLETA)

✅ FIX: Agora busca TODAS as páginas de cada categoria, não só a primeira
"""

import os
import sys
import requests
from datetime import datetime
from supabase import create_client, Client

# Configuração
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Categorias SuperBid para monitorar
SUPERBID_CATEGORIES = [
    'alimentos-e-bebidas',
    'animais',
    'artes-decoracao-colecionismo',
    'bolsas-canetas-joias-e-relogios',
    'caminhoes-onibus',
    'carros-motos',
    'cozinhas-e-restaurantes',
    'eletrodomesticos',
    'embarcacoes-aeronaves',
    'imoveis',
    'industrial-maquinas-equipamentos',
    'maquinas-pesadas-agricolas',
    'materiais-para-construcao-civil',
    'moveis-e-decoracao',
    'movimentacao-transporte',
    'oportunidades',
    'sucatas-materiais-residuos',
    'tecnologia',
]


class SuperBidMonitor:
    """Monitor de lances SuperBid com paginação completa"""
    
    def __init__(self):
        """Inicializa conexões"""
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidas")
        
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "*/*",
            "accept-language": "pt-BR,pt;q=0.9",
            "origin": "https://exchange.superbid.net",
            "referer": "https://exchange.superbid.net/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        
        # Cache: {link: {category, source, external_id, lot_number}}
        self.db_items = {}
    
    def load_database_items(self):
        """Carrega TODOS os itens ativos do banco indexados por link"""
        print("📥 Carregando itens do banco (SuperBid ativos)...")
        
        try:
            page_size = 1000
            offset = 0
            total_loaded = 0
            
            while True:
                response = self.supabase.schema("auctions").table("vw_auctions_unified")\
                    .select("link,category,source,external_id,lot_number")\
                    .eq("source", "superbid")\
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
                        }
                
                total_loaded += len(response.data)
                print(f"   → Carregados {total_loaded} itens...")
                
                if len(response.data) < page_size:
                    break
                
                offset += page_size
            
            print(f"✅ {len(self.db_items)} itens SuperBid carregados da view")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao carregar itens: {e}")
            return False
    
    def fetch_superbid_category(self, category: str, page_size: int = 100, max_pages: int = 100):
        """
        ✅ FIX: Busca TODAS as páginas de ofertas de uma categoria
        
        Args:
            category: Nome da categoria
            page_size: Itens por página (padrão 100)
            max_pages: Máximo de páginas a buscar (padrão 100)
            
        Returns:
            Lista com todas as ofertas encontradas
        """
        all_offers = []
        page_num = 1
        consecutive_errors = 0
        max_errors = 3
        
        print(f"   Buscando páginas", end='', flush=True)
        
        while page_num <= max_pages and consecutive_errors < max_errors:
            try:
                params = {
                    "urlSeo": f"https://exchange.superbid.net/categorias/{category}",
                    "locale": "pt_BR",
                    "orderBy": "score:desc",
                    "pageNumber": page_num,  # ✅ Agora incrementa!
                    "pageSize": page_size,
                    "portalId": "[2,15]",
                    "requestOrigin": "marketplace",
                    "searchType": "openedAll",
                    "timeZoneId": "America/Sao_Paulo",
                }
                
                response = self.session.get(
                    "https://offer-query.superbid.net/seo/offers/",
                    params=params,
                    timeout=30
                )
                
                # Status 404 = fim das páginas
                if response.status_code == 404:
                    break
                
                if response.status_code != 200:
                    consecutive_errors += 1
                    if consecutive_errors >= max_errors:
                        break
                    continue
                
                data = response.json()
                offers = data.get("offers", [])
                
                # Página vazia = fim
                if not offers:
                    break
                
                all_offers.extend(offers)
                print(f" {page_num}", end='', flush=True)
                
                # Menos que page_size = última página
                if len(offers) < page_size:
                    break
                
                page_num += 1
                consecutive_errors = 0
                
            except requests.exceptions.JSONDecodeError:
                consecutive_errors += 1
                if consecutive_errors >= max_errors:
                    break
                page_num += 1
                
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= max_errors:
                    break
                page_num += 1
        
        print(f" → {len(all_offers)} ofertas")
        return all_offers
    
    def process_offer(self, offer):
        """Processa uma oferta e retorna dados para histórico"""
        offer_id = offer.get("id")
        if not offer_id:
            return None
        
        # Monta URL da oferta
        link = f"https://exchange.superbid.net/oferta/{offer_id}"
        
        # Verifica se esse link existe no banco
        db_item = self.db_items.get(link)
        if not db_item:
            return None
        
        # Extrai dados de lances da API
        total_bids = offer.get("totalBids", 0)
        total_bidders = offer.get("totalBidders", 0)
        
        detail = offer.get("offerDetail", {})
        current_value = detail.get("currentMinBid") or detail.get("initialBidValue")
        
        # Retorna dados combinados: info do banco + lances da API
        return {
            "category": db_item["category"],
            "source": db_item["source"],
            "external_id": db_item["external_id"],
            "lot_number": db_item["lot_number"],
            "total_bids": total_bids,
            "total_bidders": total_bidders,
            "current_value": current_value,
            "captured_at": datetime.now().isoformat(),
        }
    
    def update_base_tables(self, records):
        """Atualiza tabelas base com dados de lances"""
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
                            "total_bids": record["total_bids"],
                            "total_bidders": record["total_bidders"],
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
                print(f"✅ {category:45s} | {cat_updated:3d} atualizados | {cat_errors:2d} erros")
            elif cat_errors > 0:
                print(f"❌ {category:45s} | 0 atualizados | {cat_errors:2d} erros")
        
        return updated_count
    
    def save_bid_history(self, records):
        """Salva histórico de lances em lote"""
        if not records:
            return 0
        
        try:
            # Remove duplicatas baseado em chave única
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
        print("🔵 SUPERBID MONITOR - HISTÓRICO DE LANCES (COM PAGINAÇÃO)")
        print("="*70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        # Carrega itens do banco
        if not self.load_database_items():
            print("❌ Falha ao carregar itens do banco")
            return False
        
        if not self.db_items:
            print("⚠️ Nenhum item ativo encontrado no banco")
            return True
        
        # Processa categorias da API
        all_records = []
        matched_count = 0
        total_offers = 0
        
        print("\n📡 Buscando ofertas da API (TODAS as páginas)...\n")
        
        for category in SUPERBID_CATEGORIES:
            print(f"📦 {category}")
            
            # ✅ Agora busca TODAS as páginas!
            offers = self.fetch_superbid_category(category)
            total_offers += len(offers)
            
            category_matches = 0
            
            for offer in offers:
                record = self.process_offer(offer)
                if record:
                    all_records.append(record)
                    category_matches += 1
            
            matched_count += category_matches
            
            if category_matches > 0:
                print(f"   ✅ {len(offers)} API | {category_matches} matches")
            else:
                print(f"   ⚪ {len(offers)} API | 0 matches")
            
            print()  # Linha em branco entre categorias
        
        # Atualiza tabelas base
        print("="*70)
        print("🔄 Atualizando tabelas base (total_bids, total_bidders, value, last_scraped_at)...")
        print("="*70)
        print()
        
        updated = self.update_base_tables(all_records)
        
        # Salva histórico
        print()
        print("="*70)
        print("💾 Salvando histórico de lances na tabela auction_bid_history...")
        print("="*70)
        
        saved = self.save_bid_history(all_records)
        
        print(f"\n✅ {saved} registros salvos no histórico")
        
        print("\n" + "="*70)
        print("📊 RESUMO DA EXECUÇÃO")
        print("="*70)
        print(f"📋 Itens SuperBid na view: {len(self.db_items)}")
        print(f"📡 Ofertas retornadas da API: {total_offers}")
        print(f"🔗 Links matched (encontrados): {matched_count}")
        print(f"🔄 Tabelas base atualizadas: {updated}")
        print(f"💾 Registros salvos no histórico: {saved}")
        print("="*70)
        
        if len(self.db_items) > 0:
            match_rate = (matched_count / len(self.db_items)) * 100
            print(f"\n📈 Taxa de match: {match_rate:.1f}%")
            
            if match_rate < 10:
                print(f"⚠️ Poucos matches! Verifique se:")
                print(f"   - Os links no banco estão no formato correto")
                print(f"   - As ofertas ainda estão ativas na API")
        
        return True


def main():
    """Execução principal"""
    try:
        monitor = SuperBidMonitor()
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