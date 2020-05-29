ARG build_image=node:12.14.1
ARG base_image=node:12.14.1-alpine

FROM $build_image AS build

LABEL maintainer="Junxiang Wei <kevinprotoss.wei@gmail.com>"

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY package*.json ./

RUN npm install
# If you are building your code for production
# RUN npm ci --only=production

# Bundle app source
COPY . .

FROM $base_image

COPY --from=build /usr/src/app/ /app/
WORKDIR /app

CMD [ "node", "index.js" ]
