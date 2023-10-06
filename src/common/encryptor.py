import os

from cryptography.fernet import Fernet

key = 'zAAacPYpDBYkcEFmt_CxfS8nviSspuJl0V_Eh1rIb8o='

def gen_key() -> bytes:
    return Fernet.generate_key()


def encrypt(message: bytes, key: bytes) -> bytes:
    return Fernet(key).encrypt(message)


def decrypt(token: bytes, key: bytes) -> bytes:
    return Fernet(key).decrypt(token)


if __name__ == '__main__':
    p = encrypt('Vpbank2023DnA'.encode(), key.encode())
    print(p.decode("utf-8") )

    # key = os.getenv('FERNET_KEY', 'zAAacPYpDBYkcEFmt_CxfS8nviSspuJl0V_Eh1rIb8o=')

    # print(decrypt('gAAAAABjtUDZtA-rC0ktEcctxtYWawKU8dPJXBFSYzDabsNQYPxcpR-F4A4hZJOF3xT6t9nRBvMW59evjnL-5OW37vsFvDXnkQ=='.encode(),
    #               'zAAacPYpDBYkcEFmt_CxfS8nviSspuJl0V_Eh1rIb8o='.encode()))
