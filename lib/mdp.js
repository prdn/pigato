module.exports = {
	WORKER: 'W',
	CLIENT: 'C',

	W_READY:      '\001',
	W_REQUEST:    '\002',
	W_REPLY:      '\003',
	W_HEARTBEAT:  '\004',
	W_DISCONNECT: '\005',
	W_REPLY_PARTIAL: '\006',
	W_REPLY_REJECT: '\007'
};
