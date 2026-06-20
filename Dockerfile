# Imagen oficial de Playwright — ya trae Chromium y todas las dependencias del sistema
FROM mcr.microsoft.com/playwright:v1.44.1-jammy

WORKDIR /app

# Copia los archivos del proyecto
COPY package.json ./
COPY server.js ./

# Instala solo las dependencias de Node (Chromium ya está en la imagen)
RUN npm install

# Expone el puerto
EXPOSE 3000

# Arranca el servidor
CMD ["node", "server.js"]
