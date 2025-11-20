from passlib.context import CryptContext
from auth_jwt import crear_access_token

# Contexto de hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Usuario de prueba (admin)
USUARIO_ADMIN = {
    "username": "admin",
    "hashed_password": pwd_context.hash("Ganbatte2025!")
}

def login_usuario(username: str, password: str):
    if username != USUARIO_ADMIN["username"]:
        return None
    if not pwd_context.verify(password, USUARIO_ADMIN["hashed_password"]):
        return None
    # Crear token JWT
    return crear_access_token({"sub": username})
