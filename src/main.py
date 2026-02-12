import sys
from pathlib import Path

# Añadir directorio src al path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scrapers.ft_scraper import FTScraper
from scrapers.fundsquare_scraper import FundsquareScraper
from utils.json_manager import JSONManager
from utils.logger import setup_logger

logger = setup_logger(__name__)

def load_funds_config(config_path="data/funds_config.txt"):
    """
    Lee funds_config.txt y devuelve lista de fondos activos
    Formato: ISIN|idInstr
    """
    funds = []
    config_file = Path(config_path)
    
    if not config_file.exists():
        logger.error(f"❌ No se encuentra {config_path}")
        return funds
    
    with open(config_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            parts = line.split('|')
            if len(parts) != 2:
                logger.warning(f"⚠️  Línea {line_num} inválida: {line}")
                continue
            
            isin, id_instr = parts[0].strip(), parts[1].strip()
            funds.append({"isin": isin, "id_instr": id_instr})
    
    logger.info(f"📋 Cargados {len(funds)} fondos desde configuración")
    return funds

def cleanup_deleted_funds(current_isins, json_manager):
    """Elimina JSONs de fondos ya no presentes en la configuración"""
    historical_path = Path("data/historical")
    if not historical_path.exists():
        return
    
    existing_files = list(historical_path.glob("*.json"))
    
    for json_file in existing_files:
        isin = json_file.stem
        if isin not in current_isins:
            json_manager.delete_fund_data(isin)

def main():
    logger.info("🚀 Iniciando actualización de precios...")
    
    # Cargar configuración
    funds = load_funds_config()
    if not funds:
        logger.error("❌ No hay fondos configurados")
        sys.exit(1)
    
    # Inicializar scrapers
    ft_scraper = FTScraper()
    fs_scraper = FundsquareScraper()
    json_manager = JSONManager()
    
    # Limpiar fondos eliminados
    current_isins = {f["isin"] for f in funds}
    cleanup_deleted_funds(current_isins, json_manager)
    
    # Procesar cada fondo
    for fund in funds:
        isin = fund["isin"]
        id_instr = fund["id_instr"]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Procesando {isin}")
        logger.info(f"{'='*60}")
        
        # Cargar datos existentes
        data = json_manager.load_data(isin)
        
        # Scraping Fundsquare (prioridad baja)
        fs_result = fs_scraper.scrape(id_instr)
        for price in fs_result["prices"]:
            json_manager.upsert_price(data, price)
        
        # Scraping Financial Times (prioridad alta - sobrescribe)
        ft_result = ft_scraper.scrape(isin)
        if ft_result["name"] and not data["name"]:
            data["name"] = ft_result["name"]
        if ft_result["currency"]:
            data["currency"] = ft_result["currency"]
        
        for price in ft_result["prices"]:
            json_manager.upsert_price(data, price)
        
        # Rotar datos antiguos (mantener 8 años)
        json_manager.rotate_old_data(data, years=8)
        
        # Guardar
        json_manager.save_data(isin, data)
    
    logger.info(f"\n✅ Proceso completado correctamente")

if __name__ == "__main__":
    main()
