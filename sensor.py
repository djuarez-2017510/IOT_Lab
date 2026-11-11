"""Simulacion de los sensores de la estacion."""
import random

DIRECCIONES = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]


def _normal(media=55, sigma=15, lo=0, hi=110):
    # gaussiana centrada en 55, recortada al rango valido
    return min(hi, max(lo, random.gauss(media, sigma)))


def generar_lectura():
    return {
        "temperatura": round(_normal(), 2),
        "humedad": int(round(_normal(hi=100))),
        "direccion_viento": random.choice(DIRECCIONES),
    }
