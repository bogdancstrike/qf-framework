__all__ = (
    'h2b', 'b2h', 'h2lb', 'b2hs',
    'b2i', 'i2b',
    'h2s', 's2h',
    'h2i', 'i2h',
    's2b', 'b2s',
    'i2s', 's2i',
    'l2s', 's2l',
    'm0h', 'h2h',
    'lhi', 'lhh', 'llh', 'loi',
    't2h', 'r0h',
    'hhh', 'mhh',
    'lshi', 'lski',
    'h2k', '_xor'
)

import re
from typing import Optional


def t2h(t: str) -> str:
    if not t:
        return None
    return b2h(t.encode())


def hhh(h: str) -> str:
    if not h or not h.strip():
        return h
    h = re.sub(r"[^0-9a-fA-F]+", '', h.strip())
    return f" {' '.join(h[i:(i + 2)] for i in range(0, len(h), 2)).strip()} " if len(h) % 2 < 1 else None


def mhh(match):
    return hhh(match.group(0))


def h2h(h: str) -> str:
    if not h or not h.strip():
        return h
    return re.sub(r'(?<=\})[^{]+(?=\{)*', mhh, '{}' + h)[2:].strip()


def h2b(h: str) -> bytes:
    return bytes.fromhex(h)


def b2h(b: bytes) -> str:
    if b is None:
        return ''
    return ' '.join([f'{i:02x}' for i in b])


def b2hs(b: bytes) -> str:
    if b is None:
        return ''
    return ''.join([f'{i:02x}' for i in b])


def h2lb(h: str):
    return list(bytes.fromhex(h))


def b2i(b: bytes) -> int:
    # return int(b2h(a).replace(' ', ''), base=16)
    return int.from_bytes(b, byteorder='big')


def i2b(i: int) -> bytes:
    return bytes.fromhex(f'{i:02x}')


def h2s(h: str) -> str:
    return ''.join([chr(i) for i in bytes.fromhex(h)])


def x2x(x):
    return f'{ord(x):02x}' if ord(x) < 256 else x.encode('utf-8').hex()


def s2h(s: str) -> str:
    if not s or s == 'None':
        return None
    return ' '.join([(f'{ord(x):02x}' if ord(x) < 256 else x.encode('utf-8').hex()) for x in s]) or None
    # return ' '.join([hex(ord(x)) for x in s]) or None
    # return f" {' '.join('{:02x}'.format(ord(c)) for c in s)}" or None
    # return hex(int.from_bytes(s.encode(), 'big'))


def h2i(h: str) -> int:
    return int.from_bytes(bytes.fromhex(h), byteorder='big')


def i2h(i: int) -> str:
    x = hex(i).lower()[2:]
    x = f'{"0"*(len(x) % 2)}{x}'
    # x = f'{i:02x}'
    return x


def s2b(s: str) -> bytes:
    return bytes.fromhex(''.join([f'{ord(x):02x}' for x in s]))


def b2s(b: bytes) -> str:
    return ''.join([chr(i) for i in b])


def i2s(i: int) -> str:
    return ''.join([chr(i) for i in bytes.fromhex(f'{i:02x}')])


def s2i(s: str) -> int:
    return sum(ord(char) << (8 * i) for i, char in enumerate(s[::-1]))


def l2s(l):
    """ Transform list of u8 to string. """
    return ''.join([chr(x) for x in l])


def s2l(s):
    """ Transform string to list of u8. """
    return [ord(x) for x in s]


def m0h(hex='', start=0, offset=0):
    return b2h(h2b(hex)[start:offset])


def h2k(hex: Optional[str] = ''):
    return [eval(i) for i in re.findall(r'{[^{(*?)}]*}', hex)]


def loi(i: int = 0, lim: Optional[int] = 255):
    if i < 1:
        return 1
    for n in range(0, 3):
        if i in range(lim ** n, lim ** (n + 1)):
            return n + 1
    return 0


def lshi(hex: Optional[str] = ''):
    # rem keys
    hex = re.sub(r'{[^{(*?)}]*}', '', hex.strip()) if hex and hex.strip() else ''
    # rem no hex
    hex = re.sub(r"[^0-9a-fA-F]+", '', hex).strip()
    hex = hex if len(hex) % 2 < 1 else ''
    return len(h2b(hex)) or 0


def lski(hex: Optional[str] = ''):
    return sum(
        obj['l'] for obj in [
            # eval string dict from extract keys
            eval(i) for i in re.findall(r'{[^{(*?)}]*}', hex)
        ] if ('l' in obj and type(obj['l']) is int)
    ) or 0


def lhi(hex: Optional[str] = ''):
    return lshi(hex) + lski(hex) or 0


def llh(i: Optional[int] = 0, lim: Optional[int] = 255, ber: Optional[bool] = False):
    l = loi(i, lim)
    return f'8{l} {i:0{2 * l}x}' if ber else (f'{i:02x}' if i < lim else (f'{i:06x}' if l < 4 else ''))


def lhh(hex: Optional[str] = '', lim: Optional[int] = 255, ber: Optional[bool] = False):
    return llh(i=lhi(hex), lim=lim, ber=ber)


def r0h(a, b):
    return ' '.join(['{:02X}'.format(x) for x in range(a, b)])


def _xor(kifd, kicc):
    # x =hex(int(kifd, 16) ^ int(kicc, 16)).upper()[2:]
    x = i2h(h2i(kifd) ^ h2i(kicc))
    return x