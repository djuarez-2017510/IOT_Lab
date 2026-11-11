"""Consumidor: lee del topic y grafica en vivo.

  python consumer.py --modo json
  python consumer.py --modo compact
"""
import argparse
import json
import uuid
from collections import deque
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

import config
from codec import decode
from sensor import DIRECCIONES


class Grafica:
    def __init__(self, titulo, archivo, max_puntos=100):
        self.archivo = archivo
        self.t = deque(maxlen=max_puntos)
        self.temp = deque(maxlen=max_puntos)
        self.hum = deque(maxlen=max_puntos)
        self.viento = deque(maxlen=max_puntos)

        plt.ion()
        self.fig, (self.ax_t, self.ax_h, self.ax_v) = plt.subplots(3, 1, figsize=(11, 8), sharex=True)
        self.fig.suptitle(titulo)

    def agregar(self, ts, lectura):
        self.t.append(ts)
        self.temp.append(lectura["temperatura"])
        self.hum.append(lectura["humedad"])
        self.viento.append(DIRECCIONES.index(lectura["direccion_viento"]))

    def dibujar(self):
        for ax in (self.ax_t, self.ax_h, self.ax_v):
            ax.clear()
            ax.grid(alpha=0.3)

        self.ax_t.plot(self.t, self.temp, "r-o", markersize=4)
        self.ax_t.set_ylabel("Temperatura (°C)")
        self.ax_t.set_ylim(0, 110)

        self.ax_h.plot(self.t, self.hum, "b-o", markersize=4)
        self.ax_h.set_ylabel("Humedad (%)")
        self.ax_h.set_ylim(0, 100)

        self.ax_v.plot(self.t, self.viento, "g-o", markersize=4, drawstyle="steps-post")
        self.ax_v.set_ylabel("Dirección viento")
        self.ax_v.set_yticks(range(len(DIRECCIONES)))
        self.ax_v.set_yticklabels(DIRECCIONES)
        self.ax_v.set_xlabel("Hora")
        self.ax_v.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

        self.fig.autofmt_xdate()
        self.fig.tight_layout(rect=(0, 0, 1, 0.96))
        self.fig.savefig(self.archivo, dpi=150)
        self.fig.canvas.draw_idle()


def parsear(modo, valor):
    if modo == "json":
        return json.loads(valor.decode())
    return decode(valor)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--modo", choices=["json", "compact"], default="json")
    ap.add_argument("--topic")
    ap.add_argument("--solo-nuevos", action="store_true", help="ignorar el historial del topic")
    args = ap.parse_args()

    topic = args.topic or (config.TOPIC_JSON if args.modo == "json" else config.TOPIC_COMPACT)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=config.BOOTSTRAP,
        group_id=f"lab-{topic}-{uuid.uuid4().hex[:6]}",
        auto_offset_reset="latest" if args.solo_nuevos else "earliest",
        **config.KAFKA_AUTH,
    )
    print(f"Consumidor listo -> topic={topic} modo={args.modo}")

    graf = Grafica(f"Estación meteorológica - topic {topic} ({args.modo})", f"captura_{args.modo}.png")

    try:
        while plt.fignum_exists(graf.fig.number):
            lote = consumer.poll(timeout_ms=1000)
            hubo_datos = False
            for mensajes in lote.values():
                for m in mensajes:
                    try:
                        lectura = parsear(args.modo, m.value)
                        graf.agregar(datetime.fromtimestamp(m.timestamp / 1000), lectura)
                        print(f"[recibido] {lectura}")
                        hubo_datos = True
                    except Exception as e:
                        print(f"mensaje descartado: {e}")
            if hubo_datos:
                graf.dibujar()
            plt.pause(0.1)
    except KeyboardInterrupt:
        print("\nDetenido por el usuario")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
