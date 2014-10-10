module.exports = {
	WORKER: 'W',
	CLIENT: 'C',

	W_READY:      0x01,
	W_REQUEST:    0x02,
	W_REPLY:      0x03,
	W_HEARTBEAT:  0x04,
	W_DISCONNECT: 0x05,
	W_REPLY_PARTIAL: 0x06
};
