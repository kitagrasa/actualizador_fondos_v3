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
    
    def get_json_path(self, isin):
        return self.base_path / f"{isin}.json"
    
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
            
            # Comparar precios con tolerancia para decimales
            price_changed = abs(existing_price_value - new_price_value) > 0.001
            
            # CASO 1: Mismo precio, misma fuente -> no hacer nada
            if not price_changed and existing_source == new_source:
                logger.debug(f"Ignorado {date_str}: precio identico de {new_source}")
                return False
            
            # CASO 2: Precio diferente y nueva fuente tiene mayor prioridad -> sobrescribir
            if price_changed and new_priority > existing_priority:
                data["prices"][existing_idx] = new_price
                logger.info(f"Actualizado {date_str}: {existing_price_value} -> {new_price_value} ({new_source} sobrescribe {existing_source})")
                return True
            
            # CASO 3: Precio diferente y nueva fuente tiene menor/igual prioridad -> mantener existente
            if price_changed and new_priority <= existing_priority:
                logger.debug(f"Mantenido {date_str}: {existing_price_value} de {existing_source} (prioridad mayor que {new_source})")
                return False
            
            # CASO 4: Mismo precio pero nueva fuente tiene mayor prioridad -> sobrescribir fuente
            if not price_changed and new_priority > existing_priority:
                data["prices"][existing_idx] = new_price
                logger.debug(f"Actualizada fuente {date_str}: {existing_source} -> {new_source}")
                return True
            
            # CASO 5: Resto de casos -> mantener existente
            logger.debug(f"Mantenido {date_str} con fuente prioritaria existente")
            return False
        else:
            # No existe -> añadir
            data["prices"].append(new_price)
            logger.info(f"Anadido {date_str}: {new_price_value} ({new_source})")
            return True
    
    def save_data(self, isin, data):
        data["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Ordenar por fecha
        data["prices"].sort(key=lambda x: x["date"])
        
        # Eliminar duplicados por fecha (mantener el de mayor prioridad)
        seen_dates = {}
        unique_prices = []
        
        for price in data["prices"]:
            date = price["date"]
            priority = price.get("priority", 0)
            
            if date not in seen_dates or priority > seen_dates[date]["priority"]:
                seen_dates[date] = price
        
        unique_prices = list(seen_dates.values())
        unique_prices.sort(key=lambda x: x["date"])
        
        data["prices"] = unique_prices
        
        json_path = self.get_json_path(isin)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Guardado {json_path.name} ({len(data['prices'])} precios)")
    
    def delete_fund_data(self, isin):
        json_path = self.get_json_path(isin)
        if json_path.exists():
            json_path.unlink()
            logger.info(f"Eliminado historico de {isin}")
            return True
        return False
