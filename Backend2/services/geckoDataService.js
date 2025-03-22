const axios = require('axios');
const cron = require('node-cron');
const mongoose = require('mongoose');
const Token = require('../models/Token');
const GeckoData = require('../models/GeckoData');

// Configuration
const GECKO_API_BASE_URL = 'https://api.geckoterminal.com/api/v2';
const UPDATE_INTERVAL = process.env.UPDATE_INTERVAL || '*/30 * * * *'; // Every 30 minutes by default
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 10; // Process tokens in batches
const PARALLEL_REQUESTS = parseInt(process.env.PARALLEL_REQUESTS) || 3; // Number of parallel requests
const REQUEST_DELAY = parseInt(process.env.REQUEST_DELAY) || 500; // Delay between requests in ms

// Fetch token pools data using contract address
async function fetchTokenPoolsData(contractAddress) {
  try {
    // Using the pools endpoint with the contract address
    const response = await axios.get(`${GECKO_API_BASE_URL}/networks/base/tokens/${contractAddress}/pools`, {
      headers: {
        'Accept': 'application/json'
      }
    });

    if (response.data && response.data.data) {
      return response.data.data;
    }
    
    return [];
  } catch (error) {
    if (error.response) {
      console.error(`Error fetching pools for ${contractAddress}: ${error.response.status} ${error.response.statusText}`);
    } else {
      console.error(`Error fetching pools for ${contractAddress}: ${error.message}`);
    }
    
    // Handle rate limiting with exponential backoff
    if (error.response && error.response.status === 429) {
      console.log('Rate limited. Will retry after delay...');
      const retryAfter = parseInt(error.response.headers['retry-after'] || '60');
      await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
      return fetchTokenPoolsData(contractAddress);
    }
    
    return [];
  }
}

// Process pool data to extract useful information
async function processAndStorePoolsData(contractAddress, pools) {
  try {
    if (pools.length === 0) {
      console.log(`No pools found for ${contractAddress}`);
      return;
    }

    // Calculate total liquidity and volume across all pools
    let totalLiquidity = 0;
    let totalVolume24h = 0;
    let weightedPrice = 0;
    let totalWeight = 0;

    for (const pool of pools) {
      const attributes = pool.attributes;
      const liquidity = parseFloat(attributes.reserve_in_usd) || 0;
      const volume = parseFloat(attributes.volume_usd?.h24) || 0;
      
      totalLiquidity += liquidity;
      totalVolume24h += volume;
      
      // Weight price by liquidity
      if (attributes.base_token_price_usd && liquidity > 0) {
        const price = parseFloat(attributes.base_token_price_usd);
        weightedPrice += price * liquidity;
        totalWeight += liquidity;
      }
    }

    // Calculate weighted average price
    const averagePrice = totalWeight > 0 ? weightedPrice / totalWeight : 0;

    // Get the main pool (usually the one with highest liquidity)
    const mainPool = pools.sort((a, b) => 
      (parseFloat(b.attributes.reserve_in_usd) || 0) - (parseFloat(a.attributes.reserve_in_usd) || 0)
    )[0];

    // Extract token data from the main pool
    const tokenData = {
      contractAddress: contractAddress.toLowerCase(),
      name: mainPool.attributes.base_token_name || '',
      symbol: mainPool.attributes.base_token_symbol || '',
      price_usd: averagePrice,
      volume_usd: totalVolume24h,
      liquidity_usd: totalLiquidity,
      pool_count: pools.length,
      main_dex: mainPool.attributes.dex_id || '',
      lastUpdated: new Date()
    };

    // Update or create the token data document
    await GeckoData.findOneAndUpdate(
      { contractAddress: tokenData.contractAddress },
      tokenData,
      { upsert: true, new: true }
    );
    
    console.log(`Successfully processed pool data for ${contractAddress}`);
  } catch (error) {
    console.error(`Error processing pools data for ${contractAddress}:`, error);
  }
}

// Process a batch of tokens with controlled parallelism
async function processBatch(tokens) {
  const results = [];
  // Process tokens in smaller chunks to control parallelism
  for (let i = 0; i < tokens.length; i += PARALLEL_REQUESTS) {
    const chunk = tokens.slice(i, i + PARALLEL_REQUESTS);
    
    // Process this chunk in parallel
    const chunkPromises = chunk.map(async (token, index) => {
      // Add staggered delay within chunk to avoid bursts
      await new Promise(resolve => setTimeout(resolve, index * REQUEST_DELAY));
      
      try {
        const pools = await fetchTokenPoolsData(token.contractAddress);
        await processAndStorePoolsData(token.contractAddress, pools);
        return { success: true, address: token.contractAddress };
      } catch (error) {
        console.error(`Failed processing token ${token.contractAddress}:`, error);
        return { success: false, address: token.contractAddress, error };
      }
    });
    
    // Wait for all tokens in this chunk to complete
    const chunkResults = await Promise.all(chunkPromises);
    results.push(...chunkResults);
    
    // Add delay between chunks within a batch
    if (i + PARALLEL_REQUESTS < tokens.length) {
      await new Promise(resolve => setTimeout(resolve, PARALLEL_REQUESTS * REQUEST_DELAY));
    }
  }
  return results;
}

async function updateTokenPoolsData() {
  console.log('Starting token pools data update...');
  
  try {
    // Get all token contract addresses from the Token collection
    const tokens = await Token.find({}, { contractAddress: 1 });
    console.log(`Found ${tokens.length} tokens to process`);
    
    // Track success/failure stats
    let successCount = 0;
    let failureCount = 0;
    
    // Process tokens in batches with controlled parallelism
    for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
      const batchTokens = tokens.slice(i, i + BATCH_SIZE);
      console.log(`Processing batch ${Math.floor(i/BATCH_SIZE) + 1} of ${Math.ceil(tokens.length/BATCH_SIZE)}`);
      
      // Process this batch
      const batchResults = await processBatch(batchTokens);
      
      // Update stats
      successCount += batchResults.filter(r => r.success).length;
      failureCount += batchResults.filter(r => !r.success).length;
      
      // Add delay between batches
      if (i + BATCH_SIZE < tokens.length) {
        console.log('Waiting between batches to avoid rate limiting...');
        await new Promise(resolve => setTimeout(resolve, 10000));
      }
    }
    
    console.log(`Token pools data update completed. Success: ${successCount}, Failed: ${failureCount}`);
  } catch (error) {
    console.error('Error updating token pools data:', error);
  }
}

function initializeDataFetching() {
  console.log('Initializing token pools data fetching service...');
  
  // Run immediately on startup with a small delay to ensure DB connection is established
  setTimeout(() => {
    updateTokenPoolsData();
  }, 5000);
  
  // Schedule regular updates with a slight offset
  const minuteOffset = Math.floor(Math.random() * 10); // 0-9 minute offset
  const cronSchedule = `${minuteOffset} */${process.env.FETCH_INTERVAL || 2} * * *`; // Every 2 hours by default
  
  cron.schedule(cronSchedule, () => {
    console.log('Running scheduled token pools data update...');
    updateTokenPoolsData();
  });
  
  console.log(`Token pools data will be updated according to schedule: ${cronSchedule}`);
}

module.exports = {
  initializeDataFetching,
  updateTokenPoolsData,
  fetchTokenPoolsData
};