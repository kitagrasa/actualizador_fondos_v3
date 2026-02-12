# Fund Price Scraper

Scraper automatizado de precios de fondos desde Fundsquare y Financial Times para Portfolio Performance.

## Características
- Ejecuta cada 19 minutos vía GitHub Actions
- Prioridad: Financial Times > Fundsquare
- Mantiene histórico completo sin límites
- Gestión simple: edita `data/funds_config.txt`

## Añadir/eliminar fondos

Edita `data/funds_config.txt`:

```txt
LU0563745743|87217
