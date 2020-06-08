FROM node:14.4

WORKDIR /app

ADD ./package.json /app/package.json
ADD ./yarn.lock /app/yarn.lock
RUN yarn install

ADD . /app
