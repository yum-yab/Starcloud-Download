from dataclasses import dataclass
import json
import base64
import requests
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5

PUBLIC_KEY_PEM = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvrzz4DGWHc6YmK0BZ30LMqZv
WTLOsuIzPJn9LrJ++5416UwqpnnR5DxI4NOAdwwAOv7aOdiZ6ny5u8BX5potv+cB3evrc
pw5HbxSbj1kUzfOv4VCnGSdPMRnx/i3DCaQN1ubliJrm/jfGBEVioTNkT+iNxcZZYxazg
P1PHJOpmUwu7LME+zdGSB+y0MIZasmKi6aVFBIHug83ku0lNpA+hdWTJu+Unsl6cD58wf
7fSF3zLbb9Cmy/kg+qcS0QzzBajSXh1UuRm+4KuQZfDRDuIagICtXvrY/u2Ow3Kdw4YGq
EMe+TLiuxFoCQO9smGCOi9sCFAVrC3DaGPhGYT422QIDAQAB
-----END PUBLIC KEY-----"""


def _encrypt_login(account: str, password: str) -> str:
    payload: dict[str, bool | str] = {
        "account": account,
        "password": password,
        "rememberMe": False,
    }
    payload_bytes: bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    rsa_key: RSA.RsaKey = RSA.import_key(PUBLIC_KEY_PEM)
    cipher: PKCS1_v1_5.PKCS115_Cipher = PKCS1_v1_5.new(rsa_key)
    encrypted_bytes = cipher.encrypt(payload_bytes)
    return base64.b64encode(encrypted_bytes).decode("utf-8")


class LoginExpired(Exception):
    def __init__(self) -> None:
        self.message = "Your login has expired"
        super().__init__(self.message)


@dataclass
class AuthData:
    id: int
    userName: str
    token: str


@dataclass
class LoginCredentials:
    email: str
    password: str


def performLogin(creds: LoginCredentials) -> AuthData:
    key: str = _encrypt_login(account=creds.email, password=creds.password)

    response: requests.Response = requests.post(
        url="https://data-starcloud.pcl.ac.cn/starcloud/api/user/authenticate",
        json={"key": key},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )

    response.raise_for_status()
    print("Login Successful")
    response_body = response.json()
    return AuthData(
        id=response_body["data"]["id"],
        userName=response_body["data"]["userName"],
        token=response_body["data"]["token"],
    )