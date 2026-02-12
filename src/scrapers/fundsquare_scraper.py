import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.logger import setup_logger

logger = setup_logger(__name__)

class FundsquareScraper:
    BASE_URL = "https://www.fundsquare.net/security/histo-prices"
    
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
        })
    
    def parse_date_fs(self, date_str):
        """
        Parsea fechas de Fundsquare en formato DD/MM/YYYY
        """
        if not date_str or not date_str.strip():
            return None
        
        try:
            dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            logger.debug(f"Fundsquare: No se pudo parsear fecha '{date_str}'")
            return None
    
    def scrape(self, id_instr):
        url = f"{self.BASE_URL}?idInstr={id_instr}"
        logger.info(f"Fundsquare: {url}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Buscar tabla de precios
            table = soup.find('table', class_='tabHorizontal')
            if not table:
                logger.warning(f"Fundsquare: Tabla no encontrada para {id_instr}")
                return {"prices": []}
            
            # Saltar primeras 2 filas (encabezados)
            all_rows = table.find_all('tr')
            rows = all_rows[2:] if len(all_rows) > 2 else []
            
            logger.info(f"Fundsquare: {len(rows)} filas encontradas")
            
            prices = []
            for idx, row in enumerate(rows):
                cols = row.find_all('td')
                
                # Validar columnas minimas
                if len(cols) < 2:
                    logger.debug(f"Fundsquare: Fila {idx} tiene solo {len(cols)} columnas")
                    continue
                
                # Extraer fecha (columna 0) y precio (columna 1)
                date_raw = cols[0].get_text(strip=True)
                price_raw = cols[1].get_text(strip=True)
                
                # Parsear fecha
                date_parsed = self.parse_date_fs(date_raw)
                if not date_parsed:
                    continue
                
                # Parsear precio
                try:
                    # Extraer parte numerica antes del espacio
                    # Ejemplo: "32.763 EUR" -> "32.763"
                    price_parts = price_raw.split()
                    if not price_parts:
                        continue
                    
                    price_part = price_parts[0]
                    
                    # Reemplazar coma europea por punto
                    price_clean = price_part.replace(',', '.').replace(' ', '').strip()
                    
                    if not price_clean or price_clean in ['0', '0.00', '-']:
                        logger.debug(f"Fundsquare: Precio invalido '{price_raw}' en {date_parsed}")
                        continue
                    
                    price_value = float(price_clean)
                    
                    # Validar rango razonable
                    if price_value < 0.01 or price_value > 1000000:
                        logger.warning(f"Fundsquare: Precio fuera de rango {price_value} en {date_parsed}")
                        continue
                    
                    prices.append({
                        "date": date_parsed,
                        "price": round(price_value, 4),
                        "source": "fundsquare",
                        "priority": 10
                    })
                    
                except (ValueError, IndexError) as e:
                    logger.debug(f"Fundsquare: Error parseando precio '{price_raw}': {e}")
                    continue
            
            logger.info(f"Fundsquare: {len(prices)} precios obtenidos")
            return {"prices": prices}
        
        except requests.exceptions.Timeout:
            logger.error(f"Fundsquare: Timeout al conectar para {id_instr}")
            return {"prices": []}
        except requests.exceptions.RequestException as e:
            logger.error(f"Fundsquare: Error de conexion para {id_instr}: {str(e)}")
            return {"prices": []}
        except Exception as e:
            logger.error(f"Fundsquare: Error inesperado para {id_instr}: {str(e)}")
            return {"prices": []}
