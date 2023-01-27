FROM node:18

# Create app directory
WORKDIR /usr/src/app

COPY . .

RUN npm install -g pnpm
RUN pnpm install
# If you are building your code for production
# RUN npm ci --only=production

ENV BROKER_HOST=$BROKER_HOST

CMD [ "node", "./src/" ]