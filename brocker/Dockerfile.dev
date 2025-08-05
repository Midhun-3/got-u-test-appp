# Stage 1: Build the TypeScript code
FROM node:18-alpine AS builder

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm install

# Copy source code
COPY . .

# Build TypeScript to JavaScript (make sure you have tsconfig.json)
RUN npm install -g typescript
RUN tsc

# Stage 2: Run the compiled JavaScript
FROM node:18-alpine

WORKDIR /app

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY package.json .

# Expose any required port if necessary (e.g., 1883 for MQTT)
# EXPOSE 1883

CMD ["node", "dist/mqtt-redis.js"]
