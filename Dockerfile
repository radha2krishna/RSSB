FROM node:20-alpine

WORKDIR /app

# Install dependencies (pdf-lib for watermarking)
COPY package.json ./
RUN npm install --omit=dev

# Copy application files
COPY bot.js ./

# Koyeb default health check port
ENV PORT=8000
EXPOSE 8000

CMD ["node", "bot.js"]
