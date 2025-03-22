const mongoose = require('mongoose');

const GeckoDataSchema = new mongoose.Schema({
  contractAddress: { 
    type: String, 
    required: true, 
    unique: true,
    lowercase: true,
    index: true
  },
  name: String,
  symbol: String,
  decimals: {
    type: Number,
    default: 18
  },
  totalSupply: String,
  price_usd: {
    type: Number,
    default: 0
  },
  volume_usd: {
    type: Number,
    default: 0
  },
  liquidity_usd: {
    type: Number,
    default: 0
  },
  market_cap: {
    type: Number,
    default: 0
  },
  pool_count: {
    type: Number,
    default: 0
  },
  main_dex: {
    type: String,
    default: ''
  },
  main_pool_address: {
    type: String,
    default: ''
  },
  main_pool_tick: {
    type: Number,
    default: 0
  },
  lastUpdated: {
    type: Date,
    default: Date.now
  }
}, { timestamps: true });

module.exports = mongoose.model('GeckoData', GeckoDataSchema);