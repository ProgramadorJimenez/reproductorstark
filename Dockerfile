FROM mcr.microsoft.com/playwright:v1.61.0-jammy

WORKDIR /app

COPY package.json ./
COPY server.js ./

RUN npm install

EXPOSE 3000

CMD ["node", "server.js"]
