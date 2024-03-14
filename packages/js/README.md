<div align="center">
  
<br>

<img src="https://raw.githubusercontent.com/ermes-labs/docs/main/docs/public/icon.png" width="30%">

<h1>@ermes-labs/redis</h1>

Javascript **bindings** for the redis storage for the [`Ermes`](https://ermes-labs.github.io/docs) framework

[![types: Typescript](https://img.shields.io/badge/types-Typescript-3178C6?style=flat-square&logo=typescript)](https://www.typescriptlang.org/)
[![Github CI](https://img.shields.io/github/actions/workflow/status/ermes-labs/client-js/ci.yml?style=flat-square&branch=main)](https://github.com/ermes-labs/client-js/actions/workflows/ci.yml)
[![Codecov](https://img.shields.io/codecov/c/github/ermes-labs/client-js?color=44cc11&logo=codecov&style=flat-square)](https://codecov.io/gh/ermes-labs/client-js)
[![code style: Biome](https://img.shields.io/badge/code_style-Biome-f7b911.svg?style=flat-square&logo=Biome)](https://biomejs.dev/)
[![npm](https://img.shields.io/npm/v/@ermes-labs/client.svg?style=flat-square)](https://www.npmjs.com/package/@ermes-labs/client)

</div>

# Introduction 📖

Ermes *(Edge-to-Cloud Resource Management for Enhanced Session-based applications)*

# Usage 

## Installation 

The module is available on [`npm`](https://www.npmjs.com/package/@ermes-labs/api).

```sh
npm install @ermes-labs/client
```

## Usage

The module exports an `ErmesClient` class.

```ts
import { ErmesClient } from "@ermes-labs/client"
```

The class act as a fetch wrapper. `ErmesClient.fetch` has the same signature of [`fetch`](https://developer.mozilla.org/en-US/docs/Web/API/fetch), but instead of accepting a complete URL, it requires only the resource path (and eventually query string) and manages the hostname internally.

```ts
// Initialize a client with options.
const client = new ErmesClient(options)
// Fetch some resources.
const response = client.fetch("/resource", fetchOptions)
```

The class constructor accept options to define how the token is extracted from the response, and the initial origin.

```ts
type ErmesClientOptions =
  | {
      // The name of the header that will contain the ermes token.
      tokenHeaderName?: string;
      // The initial origin will be set to "window.location.origin"
    }
  | {
      // The name of the header that will contain the ermes token.
      tokenHeaderName?: string;
      // The initial origin. New tokens may update the host value.
      initialOrigin: string | URL;
    }
  | {
      // The name of the header that will contain the ermes token.
      tokenHeaderName?: string;
      // The protocol to use.
      scheme?: "http" | "https";
      // Init the client with a token. Useful if a session is already present.
      initialToken: SessionToken;
    };
```

# Response headers 📖

On the server, the following response headers must be set:

- Access-Control-Allow-Origin: This header must not be the wildcard '\*' when responding to credentialed requests. Instead, it must specify the allowed origin explicitly or reflect the Origin header from the request.

- Access-Control-Allow-Credentials: This header must be set to true to tell the browser that the server allows credentials for a cross-origin request. If this header is missing, the browser will not expose the response to the frontend JavaScript code, and it will not send credentials in future requests to the server.

- Access-Control-Allow-Headers: This header is used in response to a preflight request which includes the Access-Control-Request-Headers to indicate which HTTP headers can be used during the actual request. This is relevant if you are sending headers other than simple headers (like Content-Type, Accept, etc.).

- Access-Control-Allow-Methods: In response to a preflight request, this header specifies the method or methods allowed when accessing the resource in question.
