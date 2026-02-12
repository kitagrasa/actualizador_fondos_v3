import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from .logger import setup_logger

logger = setup_logger(__name__)

SOURCE_PRIORITY = {"ft": 20, "fundsquare": 10}

class JSONManager:
    def __init__(self, base_path="data/historical"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def get_json_path(self, isin):
        """Ruta del archivo JSON para un ISIN"""
        return self.base_path / f"{isin}.json"
    
    def load_data(self, isin):
        """Carga datos existentes o crea estructura nueva"""
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
        """
        Inserta o actualiza precio aplicando prioridad de fuente
        new_price = {
            "date": "2026-02-10",
            "price": 32.828,
            "source": "ft",
            "priority": 20
        }
        """
        date_str = new_price["date"]
        existing_idx = None
        
        for idx, price in enumerate(data["prices"]):
            if price["date"] == date_str:
                existing_idx = idx
                break
        
        if existing_idx is not None:
            existing_priority = data["prices"][existing_idx].get("priority", 0)
            if new_price["priority"] > existing_priority:
                data["prices"][existing_idx] = new_price
                logger.info(f"✓ Actualizado {date_str}: {new_price['price']} ({new_price['source']} sobrescribe)")
                return True
            else:
                logger.debug(f"Mantenido {date_str} con fuente prioritaria existente")
                return False
        else:
            data["prices"].append(new_price)
            logger.info(f"✓ Añadido {date_str}: {new_price['price']} ({new_price['source']})")
            return True
    
    def rotate_old_data(self, data, years=8):
        """Mantiene solo últimos 8 años de datos"""
        cutoff_date = (datetime.now() - timedelta(days=years*365.25)).strftime("%Y-%m-%d")
        original_count = len(data["prices"])
        
        data["prices"] = [p for p in data["prices"] if p["date"] >= cutoff_date]
        data["prices"].sort(key=lambda x: x["date"])
        
        removed = original_count - len(data["prices"])
        if removed > 0:
            logger.info(f"Rotados {removed} precios anteriores a {cutoff_date}")
    
    def save_data(self, isin, data):
        """Guarda datos en JSON"""
        data["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        json_path = self.get_json_path(isin)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 Guardado {json_path.name} ({len(data['prices'])} precios)")
    
    def delete_fund_data(self, isin):
        """Elimina archivo JSON de un fondo"""
        json_path = self.get_json_path(isin)
        if json_path.exists():
            json_path.unlink()
            logger.info(f"🗑️  Eliminado histórico de {isin}")
            return True
        return False
