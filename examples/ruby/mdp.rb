#!/usr/bin/env ruby

# Majordomo Protocol Definitions

module MDP
  #  This is the version of MDP/Client we implement
  C_CLIENT = "C"

  #  This is the version of MDP/Worker we implement
  W_WORKER = "W"

  #  MDP/Server commands, as strings
  W_READY        =  "0x01"
  W_REQUEST      =  "0x02"
  W_REPLY        =  "0x03"
  W_HEARTBEAT    =  "0x04"
  W_DISCONNECT   =  "0x05"

end
