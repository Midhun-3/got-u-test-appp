FROM node:18-alpine

WORKDIR /app

# Copy package.json and install dependencies
COPY package*.json ./
RUN npm install

# Install ts-node globally
RUN npm install -g ts-node typescript

# Copy all app files
COPY . .

# Start the app
CMD ["npm", "run", "start"]
