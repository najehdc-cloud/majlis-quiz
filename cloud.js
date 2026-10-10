// Majlis Quiz — tiny Firebase client (Anonymous Auth + Realtime Database REST & streaming).
// No SDK needed, plain ES5 so it runs on older TV browsers.
// Expects window.MQ_CONFIG = { apiKey, databaseURL, playUrl } (see config.js).
(function () {
  "use strict";
  var C = window.MQ_CONFIG || {};
  var AUTH = C.authBase || "https://identitytoolkit.googleapis.com";
  var TOKEN = C.tokenBase || "https://securetoken.googleapis.com";
  var DB = (C.databaseURL || "").replace(/\/+$/, "");

  function lsGet(k) { try { return JSON.parse(localStorage.getItem(k) || "null"); } catch (e) { return null; } }
  function lsSet(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (e) {} }

  function postJson(url, body, form) {
    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": form ? "application/x-www-form-urlencoded" : "application/json" },
      body: form ? body : JSON.stringify(body)
    }).then(function (r) {
      return r.json().then(function (j) { if (!r.ok) throw new Error((j.error && (j.error.message || j.error)) || ("HTTP " + r.status)); return j; });
    });
  }

  function Client(storageKey) {
    this.key = storageKey;
    this.uid = null; this.token = null; this.refreshToken = null; this.expires = 0;
    this.streams = [];
  }

  Client.prototype.enabled = function () { return !!(C.apiKey && DB); };

  Client.prototype.signIn = function () {
    var self = this, saved = lsGet(this.key);
    if (saved && saved.refreshToken) {
      self.uid = saved.uid; self.refreshToken = saved.refreshToken;
      return self.refresh().catch(function () { return self._signUp(); });
    }
    return self._signUp();
  };

  Client.prototype._signUp = function () {
    var self = this;
    return postJson(AUTH + "/v1/accounts:signUp?key=" + encodeURIComponent(C.apiKey), { returnSecureToken: true })
      .then(function (r) {
        self.uid = r.localId; self.token = r.idToken; self.refreshToken = r.refreshToken;
        self.expires = Date.now() + (parseInt(r.expiresIn, 10) || 3600) * 1000;
        lsSet(self.key, { uid: self.uid, refreshToken: self.refreshToken });
        self._schedule();
        return self.uid;
      });
  };

  Client.prototype.refresh = function () {
    var self = this;
    return postJson(TOKEN + "/v1/token?key=" + encodeURIComponent(C.apiKey),
      "grant_type=refresh_token&refresh_token=" + encodeURIComponent(self.refreshToken), true)
      .then(function (r) {
        self.token = r.id_token; self.refreshToken = r.refresh_token; self.uid = r.user_id || self.uid;
        self.expires = Date.now() + (parseInt(r.expires_in, 10) || 3600) * 1000;
        lsSet(self.key, { uid: self.uid, refreshToken: self.refreshToken });
        self._schedule();
        // reconnect live streams with the new token
        self.streams.forEach(function (s) { s.reconnect(); });
        return self.uid;
      });
  };

  Client.prototype._schedule = function () {
    var self = this;
    clearTimeout(self._rt);
    self._rt = setTimeout(function () { self.refresh().catch(function () {}); },
      Math.max(60000, self.expires - Date.now() - 5 * 60000));
  };

  Client.prototype.url = function (path) {
    return DB + "/" + path.replace(/^\/+/, "") + ".json?auth=" + encodeURIComponent(this.token);
  };

  // method: GET | PUT | POST | PATCH | DELETE.  Retries once after refreshing the token.
  Client.prototype.req = function (method, path, body, retried) {
    var self = this;
    var opt = { method: method, cache: "no-store" };
    if (body !== undefined) { opt.body = JSON.stringify(body); opt.headers = { "Content-Type": "application/json" }; }
    return fetch(self.url(path), opt).then(function (r) {
      if (r.status === 401 && !retried) return self.refresh().then(function () { return self.req(method, path, body, true); });
      return r.text().then(function (t) {
        var j = null; try { j = t ? JSON.parse(t) : null; } catch (e) {}
        if (!r.ok) { var err = new Error((j && j.error) || ("HTTP " + r.status)); err.status = r.status; throw err; }
        return j;
      });
    });
  };

  // Live updates for a path. onData(fullValue) is called with the whole current value every time it changes.
  Client.prototype.watch = function (path, onData, onState) {
    var self = this, value = null, es = null, closed = false, retryT = null;
    function setAt(p, data, merge) {
      var parts = p.split("/").filter(function (x) { return x; });
      if (!parts.length) {
        if (merge && value && typeof value === "object" && data) { for (var k in data) { if (data[k] === null) delete value[k]; else value[k] = data[k]; } }
        else value = data;
        return;
      }
      if (!value || typeof value !== "object") value = {};
      var o = value;
      for (var i = 0; i < parts.length - 1; i++) { if (!o[parts[i]] || typeof o[parts[i]] !== "object") o[parts[i]] = {}; o = o[parts[i]]; }
      var last = parts[parts.length - 1];
      if (merge && data && typeof data === "object") {
        if (!o[last] || typeof o[last] !== "object") o[last] = {};
        for (var m in data) { if (data[m] === null) delete o[last][m]; else o[last][m] = data[m]; }
      } else if (data === null) delete o[last];
      else o[last] = data;
    }
    function handle(merge) {
      return function (ev) {
        var msg; try { msg = JSON.parse(ev.data); } catch (e) { return; }
        if (!msg) return;
        setAt(msg.path || "/", msg.data, merge);
        onData(value);
      };
    }
    function open() {
      if (closed) return;
      if (es) { try { es.close(); } catch (e) {} }
      es = new EventSource(self.url(path));
      es.addEventListener("put", handle(false));
      es.addEventListener("patch", handle(true));
      es.addEventListener("auth_revoked", function () { self.refresh().catch(function () {}); });
      es.addEventListener("cancel", function () { if (onState) onState("cancel"); });
      es.onopen = function () { if (onState) onState("open"); };
      es.onerror = function () {
        if (onState) onState("error");
        // EventSource retries by itself, but a dead token needs a fresh URL.
        if (es && es.readyState === 2) { clearTimeout(retryT); retryT = setTimeout(open, 2000); }
      };
    }
    var handleObj = {
      reconnect: open,
      close: function () { closed = true; clearTimeout(retryT); if (es) es.close(); var i = self.streams.indexOf(handleObj); if (i >= 0) self.streams.splice(i, 1); }
    };
    self.streams.push(handleObj);
    open();
    return handleObj;
  };

  Client.prototype.serverTimestamp = { ".sv": "timestamp" };

  window.MQCloud = { Client: Client, config: C };
})();
