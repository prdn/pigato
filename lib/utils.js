module.exports = {
  mparse: function(_msg) {
    var msg = [];
    for (var i = 0; i < _msg.length; i++) {
      if (_msg[i]) {
        msg[i] = _msg[i].toString();
      }
    }
    return msg;
  },
  args: function(_args) {
    var args = [];
    for (var i = 0; i < _args.length; i++) {
      args.push(_args[i]);
    }
    return args;
  }
};
