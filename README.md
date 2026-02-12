# actualizador_fondos_v3
Actualizacion automatica de precios de fondos
# 📊 Fund Price Scraper

Scraper automatizado de precios de fondos de inversión desde **Fundsquare** y **Financial Times** para Portfolio Performance.

## 🎯 Características
- ✅ Ejecuta cada 19 minutos vía GitHub Actions
- ✅ Prioridad automática: Financial Times > Fundsquare
- ✅ Mantiene histórico de 8 años
- ✅ Gestión ultra-simple: edita `data/funds_config.txt`
- ✅ JSONs compatibles con Portfolio Performance

## 📝 Cómo añadir/eliminar fondos

**Edita** `data/funds_config.txt`:

```txt
# Para AÑADIR: añade línea con formato ISIN|idInstr
LU0563745743|87217

# Para ELIMINAR: borra la línea completa
