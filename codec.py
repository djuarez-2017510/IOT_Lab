"""Payload de 3 bytes (24 bits).

  | temperatura 14 bits | humedad 7 bits | viento 3 bits |

La temperatura se guarda como entero en centesimas (0..11000),
y 11000 cabe en 14 bits porque 2^14 = 16384.
"""
from sensor import DIRECCIONES

PAYLOAD_BYTES = 3


def encode(lectura: dict) -> bytes:
    temp = round(lectura["temperatura"] * 100)
    temp = max(0, min(11000, temp))
    hum = max(0, min(100, int(lectura["humedad"])))
    viento = DIRECCIONES.index(lectura["direccion_viento"])
    valor = (temp << 10) | (hum << 3) | viento
    return valor.to_bytes(PAYLOAD_BYTES, "big")


def decode(datos: bytes) -> dict:
    if len(datos) != PAYLOAD_BYTES:
        raise ValueError(f"payload de {len(datos)} bytes, se esperaban {PAYLOAD_BYTES}")
    valor = int.from_bytes(datos, "big")
    return {
        "temperatura": round(((valor >> 10) & 0x3FFF) / 100, 2),
        "humedad": (valor >> 3) & 0x7F,
        "direccion_viento": DIRECCIONES[valor & 0x7],
    }
