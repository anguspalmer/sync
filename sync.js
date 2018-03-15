//wait on all provided promises and insert
//results back into the original array
exports.wait = async (doMap, promises) => {
  //single arg?
  if (promises === undefined && Array.isArray(doMap)) {
    promises = doMap;
    doMap = true; //in most cases, we dont need the promise after its resolved
  } else if (!Array.isArray(promises)) {
    throw `Expected array of promises`;
  }
  return new Promise((resolve, reject) => {
    let n = promises.length;
    if (n === 0) {
      resolve(promises);
      return;
    }
    let done = false;
    promises.forEach((promise, i) => {
      //wipe from array
      promises[i] = undefined;
      //store results
      promise.then(
        result => {
          if (doMap) {
            promises[i] = result;
          }
          n--;
          if (!done && n === 0) {
            done = true;
            resolve(promises);
          }
        },
        err => {
          if (!done) {
            done = true;
            reject(err);
          }
        }
      );
    });
  });
};

//normal Array.map with async concurrency.
//doMap controls whether the returned results are kept.
//async "series" is equivalent to setting concurrency to 1.
exports.loop = async function(doMap, concurrency, arr, fn) {
  //omit concurrency use maximum
  if (Array.isArray(concurrency) && typeof arr === "function") {
    fn = arr;
    arr = concurrency;
    concurrency = Infinity;
  }
  //validate
  if (typeof concurrency !== "number") throw "expecting concurrency integer";
  if (!Array.isArray(arr)) throw "expecting array";
  if (typeof fn !== "function") throw "expecting map function";
  //case, concurrency >= size
  //equiv to map->promise.all
  if (concurrency >= arr.length) {
    let promises = arr.map((d, i) => {
      let promise = fn(d, i);
      if (!promise || !(promise instanceof Promise)) {
        throw new Error("Function must return a promise");
      }
      return promise;
    });
    return await exports.wait(doMap, promises);
  }
  //case, concurrency <= size
  //need workers and a queue.
  //start!
  let queue = new Array(arr.length);
  async function worker() {
    //worker looks through all entries,
    //skipping entries already being worked on
    for (let i = 0; i < arr.length; i++) {
      if (queue[i]) {
        continue;
      }
      queue[i] = true;
      let d = arr[i];
      let promise = fn(d, i);
      if (!promise || !(promise instanceof Promise)) {
        throw new Error("Function must return a promise");
      }
      let result = await promise;
      //replace value with result
      if (doMap) {
        arr[i] = result;
      }
    }
  }
  //start <concurrency> many workers
  let workers = [];
  for (let w = 1; w <= concurrency; w++) {
    workers.push(worker());
  }
  await Promise.all(workers);
  //done!
  return arr;
};

//aliases for loop (only map edits the original array)
exports.map = exports.loop.bind(null, true);
exports.each = exports.loop.bind(null, false);
exports.forEach = exports.each;

//provide a number instead of array => each(concurrency, [0..n), fn)
exports.times = async (concurrency, n, fn) => {
  let indices = new Array(n);
  for (let i = 0; i < n; i++) {
    indices[i] = i;
  }
  return await exports.each(concurrency, indices, fn);
};

//token bucket implements a rate limiter.
//size represents the maximum number of tokens
//that can be handed out at any given time.
exports.TokenBucket = class TokenBucket {
  constructor(size) {
    this.tokens = {};
    this.numTokens = 0;
    this.maxTokens = size;
    this.queue = [];
  }
  async take() {
    if (this.numTokens === this.maxTokens) {
      let dequeue = null;
      let promise = new Promise(d => (dequeue = d));
      this.queue.push(dequeue);
      await promise;
    }
    var t = Symbol();
    this.tokens[t] = true;
    this.numTokens++;
    return this.put.bind(this, t);
  }
  put(t) {
    if (!this.tokens[t]) {
      throw "invalid token";
    }
    delete this.tokens[t];
    this.numTokens--;
    if (this.queue.length > 0) {
      let dequeue = this.queue.shift();
      dequeue();
    }
    return true;
  }
};

exports.defer = async (run, after) => {
  try {
    let p = run();
    return p && p.then ? await p : p;
  } catch (err) {
    throw err;
  } finally {
    let p = after();
    if (p && p.then) {
      await p;
    }
  }
};

exports.sleep = async ms => new Promise(r => setTimeout(r, ms));

//promisify a function. unlike node's
//util.promisify, this still calls the original
//callback. return values are lost since a promise
//must be returned.
exports.promisify = function(fn) {
  return function wrapped() {
    //store promise on callback
    var resolve, reject;
    var promise = new Promise(function(res, rej) {
      resolve = res;
      reject = rej;
    });
    //
    var args = Array.from(arguments);
    var callback = args[args.length - 1];
    var hasCallback = typeof callback === "function";
    //resolve/reject promise
    var fullfilled = function(err, data) {
      if (err) {
        reject(err);
      } else {
        var datas = Array.prototype.slice.call(arguments, 1);
        if (datas.length >= 2) {
          resolve(datas);
        } else {
          resolve(data);
        }
      }
      if (hasCallback) {
        callback.apply(this, arguments);
      }
    };
    //replace/add callback
    if (hasCallback) {
      args[args.length - 1] = fullfilled;
    } else {
      args.push(fullfilled);
    }
    //call underlying function
    var returned = fn.apply(this, args);
    if (typeof returned !== "undefined") {
      // console.log("promisify warning: discarded return value", returned);
    }
    //return promise!
    return promise;
  };
};

if (require.main === module) {
  (async function main() {
    console.log("TEST SYNC");
    let arr = [];
    for (let i = 0; i < 25; i++) {
      arr[i] = i + 1;
    }
    arr = await exports.map(15, arr, async n => {
      await exports.sleep(Math.random() * 1000);
      let v = n * 10;
      console.log(v);
      return v;
    });
    console.log("DONE", arr.length);
  })();
}
