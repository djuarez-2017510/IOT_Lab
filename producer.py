"""Productor: manda lecturas al broker cada 15-30 s hasta Ctrl+C.

  python producer.py --modo json
  python producer.py --modo compact
"""
import argparse
import json
import random
import time

from kafka import KafkaProducer

import config
from codec import encode
from sensor import generar_lectura


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--modo", choices=["json", "compact"], default="json")
    ap.add_argument("--topic", help="si no se indica se usa el de config.py")
    ap.add_argument("--min", type=float, default=15, help="segundos minimos entre envios")
    ap.add_argument("--max", type=float, default=30, help="segundos maximos entre envios")
    args = ap.parse_args()

    topic = args.topic or (config.TOPIC_JSON if args.modo == "json" else config.TOPIC_COMPACT)

    producer = KafkaProducer(
        bootstrap_servers=config.BOOTSTRAP,
        key_serializer=str.encode,
        **config.KAFKA_AUTH,
    )
    print(f"Productor listo -> {config.BOOTSTRAP} topic={topic} modo={args.modo}")

    try:
        while True:
            lectura = generar_lectura()
            if args.modo == "json":
                valor = json.dumps(lectura).encode()
            else:
                valor = encode(lectura)

            meta = producer.send(topic, key=config.SENSOR_KEY, value=valor).get(timeout=15)
            print(f"[enviado] {lectura} ({len(valor)} bytes) part={meta.partition} off={meta.offset}")
            time.sleep(random.uniform(args.min, args.max))
    except KeyboardInterrupt:
        print("\nDetenido por el usuario")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
