# j-star

[![](https://img.shields.io/npm/dm/j-star?labelColor=4a148c&color=9c27b0&style=flat)](https://www.npmjs.com/package/j-star)

A javascript Node.js package to interface with j-star.

## Installation

```
npm install --save-dev j-star
```

## Quick Start

### Connect to a j Process

```javascript
const { JConnection } = require("j-star");
const j = new JConnection({ port: 1800 });
j.connect((err) => {
  if (err) throw err;
  console.log("connected");
  // send query from here
});
```

### Connect to a TLS-protected j Process

```javascript
const { JConnection } = require("j-star");
const j = new JConnection({ port: 1800, useTLS: true });
j.connect((err) => {
  if (err) throw err;
  console.log("connected");
  // send query from here
});
```

### Connect to a j Process with Credentials

```javascript
const { JConnection } = require("j-star");
const j = new JConnection({ port: 1800, user: "user", password: "password" });
j.connect((err) => {
  if (err) throw err;
  console.log("connected");
  // send query from here
});
```

### Send a Sync Query

```javascript
j.sync("sum range 10", (err, res) => {
  if (err) throw err;
  console.log("result: ", res);
  // result: 45
});
```

### Send a Sync Function Call

```javascript
j.sync(["+", 3, 8], (err, res) => {
  if (err) throw err;
  console.log("result: ", res);
  // result: 11
});
```

### Send an Async Query

```javascript
j.asyn("show 99", (err) => {
  if (err) throw err;
});
```

### Send an Async Function Call

```javascript
j.asyn(["show", 99], (err) => {
  if (err) throw err;
});
```

### Subscribe

```javascript
j.on("upd", (table, data) => {
  console.log(table, data);
});

j.sync("sub[`trade`quote]", (err, _res) => {
  if (err) throw err;
});
```

### Close j Connection

```javascript
j.close(() => {
  console.log("closed");
});
```

## Date Types

### Deserialization

Deserialization of long and timestamp can be controlled by JConnection arguments `useBigInt` and `includeNanosecond`.

| j type    | javascript type                                    |
| --------- | -------------------------------------------------- |
| boolean   | Boolean                                            |
| u8        | Number                                             |
| i16       | Number                                             |
| i32       | Number                                             |
| i64       | if useBigInt is true then BigInt else Number       |
| f32       | Number                                             |
| f64       | Number                                             |
| symbol    | String                                             |
| timestamp | if includeNanosecond is true then Date else String |
| date      | Date                                               |
| datetime  | Date                                               |
| duration  | String                                             |
| time      | String                                             |
| dict      | Map                                                |
| list      | Array                                              |
| dataframe | Table                                              |
| function  | String                                             |

### Serialization

#### Atom

| javascript type | j type                           |
| --------------- | -------------------------------- |
| Boolean         | boolean                          |
| BigInt          | i64                              |
| Number          | i64 if isInteger(value) else f64 |
| String          | string                           |
| Date            | datetime                         |
| null            | null                             |

#### Collection

| javascript type     | j type    |
| ------------------- | --------- |
| Array               | list      |
| Table(apache-arrow) | dataframe |
| Map or Object       | dict      |
