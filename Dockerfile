FROM node:22-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci --omit=dev

COPY src/ ./src/

RUN mkdir -p /app/logs

EXPOSE 8000

CMD ["node", "src/index.js"]
