const axios = require('axios');
const cron = require('node-cron');
const mongoose = require('mongoose');
const Token = require('../models/Token');
const GeckoData = require('../models/GeckoData');

// Configuration
const UPDATE_INTERVAL = process.env.UPDATE_INTERVAL || '*/30 * * * *';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100; // The Graph can handle larger batches

// We'll use Uniswap's Base subgraph as an example
// You may want to use other DEX subgraphs like SushiSwap, Pancake, etc.
const GRAPH_API_URL = 'https://api.thegraph.com/subgraphs/name/messari/uniswap-v3-base';

// This query gets pool data for multiple tokens in a single request
function buildPoolsQuery(tokenAddresses) {
  const addressesLowerCase = tokenAddresses.map(addr => addr.toLowerCase());
  
  return {
    query: `
      query PoolsData {
        tokens(where: {id_in: [${addressesLowerCase.map(addr => `"${addr}"`).join(', ')}]}, first: 500) {
          id
          name
          symbol
          decimals
          totalValueLockedUSD
          volume24h: volume24hUSD
          volumeWeek: volumeWeekUSD
          totalSupply
          tokenDayData(first: 1, orderBy: date, orderDirection: desc) {
            priceUSD
            date
          }
          pools(first: 10, orderBy: totalValueLockedUSD, orderDirection: desc) {
            id
            totalValueLockedUSD
            volumeUSD
            feeTier
            token0 {
              id
              symbol
            }
            token1 {
              id
              symbol
            }
          }
        }
      }
    `
  };
}

// Function to fetch data from The Graph
async function fetchTokensDataFromGraph(tokenAddresses) {
  try {
    console.log(`Fetching data for ${tokenAddresses.length} tokens from The Graph...`);
    
    const query = buildPoolsQuery(tokenAddresses);
    
    const response = await axios.post(GRAPH_API_URL, query, {
      headers: {
        'Content-Type': 'application/json',
      }
    });

    if (response.data && response.data.data && response.data.data.tokens) {
      return response.data.data.tokens;
    }
    
    return [];
  } catch (error) {
    console.error('Error fetching data from The Graph:', error.message);
    
    // Add retry logic for network errors
    if (error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT') {
      console.log('Connection error. Retrying after delay...');
      await new Promise(resolve => setTimeout(resolve, 5000));
      return fetchTokensDataFromGraph(tokenAddresses);
    }
    
    // The Graph has generous rate limits, but just in case
    if (error.response && error.response.status === 429) {
      console.log('Rate limited. Retrying after delay...');
      await new Promise(resolve => setTimeout(resolve, 30000));
      return fetchTokensDataFromGraph(tokenAddresses);
    }
    
    return [];
  }
}

// Process data from The Graph and store in database
async function processAndStoreGraphData(tokensData) {
  try {
    if (!tokensData || tokensData.length === 0) {
      console.log('No tokens data to process from The Graph');
      return;
    }

    console.log(`Processing data for ${tokensData.length} tokens from The Graph...`);
    
    for (const token of tokensData) {
      // Calculate total liquidity across all pools
      let totalLiquidity = parseFloat(token.totalValueLockedUSD) || 0;
      
      // If token doesn't have direct TVL, sum up from pools
      if (!totalLiquidity && token.pools && token.pools.length > 0) {
        totalLiquidity = token.pools.reduce((sum, pool) => {
          return sum + (parseFloat(pool.totalValueLockedUSD) || 0);
        }, 0);
      }
      
      // Get current price from tokenDayData
      const currentPrice = token.tokenDayData && token.tokenDayData.length > 0 
        ? parseFloat(token.tokenDayData[0].priceUSD) || 0 
        : 0;
      
      // Find main pool (highest liquidity)
      let mainDex = "unknown";
      let highestLiquidity = 0;
      
      if (token.pools && token.pools.length > 0) {
        token.pools.forEach(pool => {
          const liquidity = parseFloat(pool.totalValueLockedUSD) || 0;
          if (liquidity > highestLiquidity) {
            highestLiquidity = liquidity;
            // For Uniswap we don't have a dex name, so using fee tier to differentiate
            mainDex = `Uniswap V3 (${pool.feeTier / 10000}%)`;
          }
        });
      }
      
      // Prepare token document
      const tokenDoc = {
        contractAddress: token.id.toLowerCase(),
        name: token.name || '',
        symbol: token.symbol || '',
        price_usd: currentPrice,
        volume_usd: parseFloat(token.volume24h) || 0,
        liquidity_usd: totalLiquidity,
        pool_count: token.pools?.length || 0,
        main_dex: mainDex,
        lastUpdated: new Date()
      };

      // Update or create GeckoData document
      await GeckoData.findOneAndUpdate(
        { contractAddress: tokenDoc.contractAddress },
        tokenDoc,
        { upsert: true, new: true }
      );
      
      console.log(`Processed token data for ${token.symbol || token.id}`);
    }
    
    console.log(`Successfully processed ${tokensData.length} tokens from The Graph`);
  } catch (error) {
    console.error('Error processing Graph data:', error);
  }
}

// Main function to update token data
async function updateTokenData() {
  console.log('Starting token data update using The Graph...');
  
  try {
    // Get all token contract addresses from the Token collection
    const tokens = await Token.find({}, { contractAddress: 1 });
    console.log(`Found ${tokens.length} tokens to process`);
    
    // Process tokens in larger batches
    for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
      const batchTokens = tokens.slice(i, i + BATCH_SIZE);
      console.log(`Processing batch ${Math.floor(i/BATCH_SIZE) + 1} of ${Math.ceil(tokens.length/BATCH_SIZE)}`);
      
      // Extract contract addresses
      const contractAddresses = batchTokens.map(token => token.contractAddress);
      
      // Fetch data for this batch
      const tokensData = await fetchTokensDataFromGraph(contractAddresses);
      
      // Process the batch data
      await processAndStoreGraphData(tokensData);
      
      // Add a small delay between batches
      if (i + BATCH_SIZE < tokens.length) {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
  } catch (error) {
    console.error('Error updating token data:', error);
  }
  
  console.log('Token data update completed using The Graph');
}

function initializeDataFetching() {
  console.log('Initializing token data fetching service using The Graph...');
  
  // Run immediately on startup with a small delay
  setTimeout(() => {
    updateTokenData();
  }, 5000);
  
  // Schedule regular updates
  const cronSchedule = `*/${process.env.FETCH_INTERVAL || 2} * * * *`; // Every 2 hours by default
  
  cron.schedule(cronSchedule, () => {
    console.log('Running scheduled token data update...');
    updateTokenData();
  });
  
  console.log(`Token data will be updated according to schedule: ${cronSchedule}`);
}

module.exports = {
  initializeDataFetching,
  updateTokenData,
  fetchTokensDataFromGraph
};