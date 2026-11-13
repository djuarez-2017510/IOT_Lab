# IoT: Estación Meteorológica (CC3067 Redes)

Daniel Juárez 23709 · Alexander de la Cruz 23735

Productor y consumidor con Kafka que simulan una estación con sensores
de temperatura, humedad y dirección del viento.

## Instalar
    python -m venv .venv
    source .venv/bin/activate      # Windows: .venv\Scripts\activate
    pip install -r requirements.txt

## Correr (dos terminales)
    python producer.py --modo json
    python consumer.py --modo json

Con la restricción de 3 bytes:

    python producer.py --modo compact
    python consumer.py --modo compact

El consumidor guarda la gráfica en `captura_json.png` / `captura_compact.png`.

## Archivos
- `sensor.py`: genera lecturas (gaussiana, media 55, sigma 15)
- `codec.py`: encode/decode del payload de 3 bytes
- `producer.py` / `consumer.py`
- `config.py`: servidor, credenciales y topics
