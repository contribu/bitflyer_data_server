FROM node:14.4

ADD . /app
WORKDIR /app
RUN npm install
