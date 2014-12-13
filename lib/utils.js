module.exports = {
  mparse: function(_msg) {
    var msg = [];
    for (var i = 0; i < _msg.length; i++) {
      if (_msg[i]) {
        msg[i] = _msg[i].toString();
      }
    }
    return msg;
  }
};
