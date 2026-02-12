import json
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.logger import setup_logger

logger = setup_logger(__name__)

SOURCE_PRIORITY = {"ft": 20, "fundsquare": 10}

class JSONManager:
    def __init__(self, base_path="data/historical"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Crear directorio para formato Portfolio Performance
        self.pp_path = Path("data/pp")
        self.pp_path.mkdir(parents=True, exist_ok=True)
    
    def get_json_path(self, isin):
        return self.base_path / f"{isin}.json"
    
    def get_pp_json_path(self, isin):
        return self.pp_path / f"{isin}.json"
    
    def load_data(self, isin):
        json_path = self.get_json_path(isin)
        
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"JSON corrupto para {isin}, creando nuevo")
        
        return {
            "isin": isin,
            "name": "",
            "currency": "",
            "prices": [],
            "last_update": None
        }
    
    def upsert_price(self, data, new_price):
        date_str = new_price["date"]
        new_price_value = new_price["price"]
        new_priority = new_price["priority"]
        new_source = new_price["source"]
        
        existing_idx = None
        
        for idx, price in enumerate(data["prices"]):
            if price["date"] == date_str:
                existing_idx = idx
                break
        
        if existing_idx is not None:
            existing_price = data["prices"][existing_idx]
            existing_price_value = existing_price["price"]
            existing_priority = existing_price.get("priority", 0)
            existing_source = existing_price.get("source", "unknown")
            
            price_changed = abs(existing_price_value - new_price_value) > 0.001
            
            if not price_changed and existing_source == new_source:
                logger.debug(f"Ignorado {date_str}: precio identico de {new_source}")
                return False
            
            if price_changed and new_priority > existing_priority:
                data["prices"][existing_idx] = new_price
                logger.info(f"Actualizado {date_str}: {existing_price_value} -> {new_price_value} ({new_source} sobrescribe {existing_source})")
                return True
            
            if price_changed and new_priority <= existing_priority:
                logger.debug(f"Mantenido {date_str}: {existing_price_value} de {existing_source}")
                return False
            
            if not price_changed and new_priority > existing_priority:
                data["prices"][existing_idx] = new_price
                logger.debug(f"Actualizada fuente {date_str}: {existing_source} -> {new_source}")
                return True
            
            logger.debug(f"Mantenido {date_str} con fuente prioritaria existente")
            return False
        else:
            data["prices"].append(new_price)
            logger.info(f"Anadido {date_str}: {new_price_value} ({new_source})")
            return True
    
    def save_data(self, isin, data):
        data["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        data["prices"].sort(key=lambda x: x["date"])
        
        seen_dates = {}
        unique_prices = []
        
        for price in data["prices"]:
            date = price["date"]
            priority = price.get("priority", 0)
            
            if date not in seen_d
