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
  let error = null;
  async function worker() {
    //worker looks through all entries,
    //skipping entries already being worked on
    for (let i = 0; i < arr.length; i++) {
      if (error) {
        break;
      }
      //if queue[i] is undefined, then job "i" is free
      //if queue[i] is true,      then job "i" is taken
      if (queue[i]) {
        continue;
      }
      queue[i] = true;
      //start!
      try {
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
      } catch (err) {
        //mark failure to ensure no more new jobs are accepted,
        //but still complete the remaining jobs inflight.
        error = true;
      }
    }
    return;
  }
  //create <concurrency> many workers
  let workers = [];
  for (let w = 1; w <= concurrency; w++) {
    workers.push(worker());
  }
  //start <concurrency> many workers,
  //blocks until all return
  await Promise.all(workers);
  //encountered an error, throw it!
  if (error !== null) {
    throw error;
  }
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
    this._tokens = {};
    this._numTokens = 0;
    this._maxTokens = size;
    this._queue = [];
  }
  get numTokens() {
    return this._numTokens;
  }
  get maxTokens() {
    return this._maxTokens;
  }
  get numQueued() {
    return this._queue.length;
  }
  get numTotal() {
    return this.numTokens + this.numQueued;
  }
  //take a token from the bucket!
  async take() {
    //bucket has no more tokens, wait for
    //one to be put back...
    if (this.numTokens === this.maxTokens) {
      let dequeue = null;
      let promise = new Promise(d => (dequeue = d));
      this._queue.push(dequeue);
      await promise;
    }
    //take token from the bucket
    var t = Symbol();
    this._numTokens++;
    this._tokens[t] = true;
    //put the token back into the bucket
    const put = () => {
      if (!this._tokens[t]) {
        throw "invalid token";
      }
      this._numTokens--;
      delete this._tokens[t];
      if (this._queue.length > 0) {
        let dequeue = this._queue.shift();
        dequeue();
      }
    };
    return put;
  }
  //wrapper around take + return
  async run(fn) {
    let put = await this.take();
    try {
      return await fn(); //proxy
    } catch (error) {
      throw error; //proxy
    } finally {
      put();
    }
  }
};

//a queue is simply a one-slot token bucket
exports.queue = () => new exports.TokenBucket(1);

//sleep(milliseconds) !
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

//run a diff against two sets.
exports.diff = async run => {
  if (!run || typeof run !== "object") {
    throw `diff: Invalid run`;
  }
  const { prev, next } = run;
  if (!Array.isArray(prev)) {
    throw `diff: Expected "prev" array`;
  } else if (!Array.isArray(next)) {
    throw `diff: Expected "next" array`;
  }
  //find "prev" indexer
  let { index, indexPrev = index, indexNext = index } = run;
  if (typeof indexPrev === "string") {
    const key = indexPrev;
    indexPrev = o => o[key];
  }
  if (typeof indexPrev !== "function") {
    throw `diff: Expected "index" function`;
  }
  //find "next" indexer
  if (typeof indexNext === "string") {
    const key = indexNext;
    indexNext = o => o[key];
  }
  if (typeof indexNext !== "function") {
    throw `diff: Expected "index" function`;
  }
  //find "equals" function,
  //by default, matching index implies matching items
  let { equal } = run;
  if (!equal) {
    equal = (p, n) => true;
  } else if (typeof equal !== "function") {
    throw `diff: Expected "equals" to be a function`;
  }
  //optional
  const { status } = run;
  //final results
  const results = {
    match: [],
    create: [],
    update: [],
    delete: []
  };
  //note and ignore duplicate
  const dupes = new Set();
  //construct join map (index-key => item)
  const join = {};
  for (const prevItem of prev) {
    const id = indexPrev(prevItem);
    if (id in join) {
      dupes.add(id);
    } else {
      join[id] = prevItem;
    }
  }
  if (dupes.size > 0) {
    results.duplicate = Array.from(dupes);
  }
  const joined = new Map();
  //compare incoming to existing
  for (const nextItem of next) {
    const id = indexNext(nextItem);
    const exists = id in join;
    if (!exists) {
      results.create.push(nextItem);
      continue;
    }
    const prevItem = join[id];
    if (equal(prevItem, nextItem)) {
      results.match.push(nextItem);
    } else {
      results.update.push(nextItem);
    }
    joined.set(nextItem, prevItem);
    delete join[id];
  }
  for (const id in join) {
    const prevItem = join[id];
    results.delete.push(prevItem);
  }
  //using results, build a set of all operations
  const operations = [];
  for (const op in results) {
    const set = results[op];
    if (!set || set.length === 0) {
      continue;
    }
    const fn = run[op];
    if (!fn) {
      continue;
    } else if (typeof fn !== "function") {
      throw `diff: Expected "${op}" to be a function`;
    }
    for (let item of set) {
      let other = joined.get(item);
      operations.push({ fn, item, other });
    }
  }
  //optionally report diff status
  if (status) status.add(operations.length);
  //execute all with <concurrency>
  const { concurrency = 1 } = run;
  await exports.each(concurrency, operations, async ({ fn, item, other }) => {
    let result = fn(item, other);
    if (result instanceof Promise) {
      await result;
    }
    //optionally report diff status
    if (status) status.done(1);
  });
  //done!
  return results;
};

if (require.main === module) {
  console.log("TEST SYNC");
  const sync = exports;
  (async function main() {
    let starts = new Set();
    let stops = new Set();
    let results = null;
    try {
      const queries = [];
      for (let i = 1; i <= 1000; i++) {
        queries.push(i);
      }
      results = await sync.map(20, queries, async i => {
        starts.add(i);
        await sync.sleep(Math.random() * 20);
        if (i === 700) {
          throw `Hit ${i}`;
        }
        stops.add(i);
        return { i };
      });
    } catch (err) {
      console.log(err);
    }
    console.log("results:", results !== null);
    console.log("starts:", starts.size);
    console.log("stops:", stops.size);
    await sync.sleep(1000);
    console.log("stops:", stops.size);
  })();
}
