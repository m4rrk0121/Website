const express = require('express');
const router = express.Router();
const GeckoData = require('../models/GeckoData');

// Get tokens with pools data
router.get('/token-pools', async (req, res) => {
  try {
    const { page = 1, limit = 20, sort = 'liquidity_usd', direction = 'desc' } = req.query;
    
    const sortOptions = {};
    sortOptions[sort] = direction === 'asc' ? 1 : -1;
    
    const skip = (page - 1) * parseInt(limit);
    
    const tokens = await GeckoData.find()
      .sort(sortOptions)
      .skip(skip)
      .limit(parseInt(limit));
      
    const totalTokens = await GeckoData.countDocuments();
    const totalPages = Math.ceil(totalTokens / parseInt(limit));
    
    res.json({
      tokens,
      totalTokens,
      totalPages,
      currentPage: parseInt(page)
    });
  } catch (err) {
    console.error('Error fetching token pools data:', err);
    res.status(500).json({ error: 'Failed to fetch token pools data' });
  }
});

// Get top liquidity and volume tokens
router.get('/top-pools', async (req, res) => {
  try {
    // Find highest liquidity token
    const topLiquidityToken = await GeckoData.findOne()
      .sort({ liquidity_usd: -1 });
    
    // Find highest volume token
    const topVolumeToken = await GeckoData.findOne()
      .sort({ volume_usd: -1 });
    
    // Find token with most pools
    const mostPoolsToken = await GeckoData.findOne()
      .sort({ pool_count: -1 });
    
    res.json({
      topLiquidityToken,
      topVolumeToken,
      mostPoolsToken
    });
  } catch (err) {
    console.error('Error fetching top pools tokens:', err);
    res.status(500).json({ error: 'Failed to fetch top pools tokens' });
  }
});

// Get specific token pools data
router.get('/token-pools/:contractAddress', async (req, res) => {
  try {
    const { contractAddress } = req.params;
    
    const token = await GeckoData.findOne({
      contractAddress: contractAddress.toLowerCase()
    });
    
    if (!token) {
      return res.status(404).json({ error: 'Token pools data not found' });
    }
    
    res.json(token);
  } catch (err) {
    console.error('Error fetching token pools data:', err);
    res.status(500).json({ error: 'Failed to fetch token pools data' });
  }
});

// Get DEX statistics
router.get('/dex-stats', async (req, res) => {
  try {
    const dexStats = await GeckoData.aggregate([
      {
        $group: {
          _id: '$main_dex',
          tokenCount: { $sum: 1 },
          totalLiquidity: { $sum: '$liquidity_usd' },
          totalVolume: { $sum: '$volume_usd' },
          avgPrice: { $avg: '$price_usd' }
        }
      },
      { $sort: { totalLiquidity: -1 } }
    ]);
    
    res.json({ dexStats });
  } catch (err) {
    console.error('Error fetching DEX statistics:', err);
    res.status(500).json({ error: 'Failed to fetch DEX statistics' });
  }
});

// Get overall pool statistics
router.get('/pool-stats', async (req, res) => {
  try {
    const stats = await GeckoData.aggregate([
      {
        $group: {
          _id: null,
          totalTokens: { $sum: 1 },
          totalLiquidity: { $sum: '$liquidity_usd' },
          totalVolume: { $sum: '$volume_usd' },
          totalPools: { $sum: '$pool_count' },
          avgPoolsPerToken: { $avg: '$pool_count' }
        }
      }
    ]);
    
    res.json(stats[0] || {
      totalTokens: 0,
      totalLiquidity: 0,
      totalVolume: 0,
      totalPools: 0,
      avgPoolsPerToken: 0
    });
  } catch (err) {
    console.error('Error fetching pool statistics:', err);
    res.status(500).json({ error: 'Failed to fetch pool statistics' });
  }
});

module.exports = router;