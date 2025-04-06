// services/tokenDataService.js
const { ethers } = require('ethers');
const axios = require('axios');
const cron = require('node-cron');
const Token = require('../models/Token');
const TokenPrice = require('../models/TokenPrice');
require('dotenv').config();

// Define multiple factory addresses
const FACTORY_ADDRESSES = [
  '0xb51F74E6d8568119061f59Fd7f98824F1e666AC1', // Original factory
  '0x9bd7dCc13c532F37F65B0bF078C8f83E037e7445', // Second factory address
  '0x05Dd3Dc91FAeFAf06499D8D7acecc5a7DecCD4be'  // Third factory address
];

// Define the TokenCreated event signature
const TOKEN_CREATED_EVENT = 'TokenCreated(address,uint256,address,string,string,uint256,address,uint256)';

// Create a configured axios instance for API calls
const geckoTerminalApi = axios.create({
  baseURL: 'https://api.geckoterminal.com/api/v2',
  timeout: 30000,
  headers: {
    'Accept': 'application/json'
  }
});

// Track API usage
let apiCallsThisMinute = 0;
let apiCallReset = null;

// Reset API call counter every minute
function setupApiCallTracking() {
  apiCallReset = setInterval(() => {
    apiCallsThisMinute = 0;
    console.log('API call counter reset');
  }, 60000);
}

// Top tokens cache and management
let topTokens = [];
const PRIORITY_TOKEN_COUNT = 10;
let lastFullUpdateTime = 0;

// Updated fetchAndStoreTokens function to handle multiple factory addresses
async function fetchAndStoreTokens() {
  try {
    console.log('Fetching tokens deployed by multiple factories...');
    
    // Create a simplified RPC provider
    const provider = new ethers.JsonRpcProvider(process.env.BASE_RPC_URL || "https://rpc.ankr.com/base");
    
    // Calculate the event topic
    const eventTopic = ethers.id(TOKEN_CREATED_EVENT);
    
    // Get the current block number
    try {
      const currentBlock = await provider.getBlockNumber();
      console.log(`Current block: ${currentBlock}`);
      
      // Focus on the last 20,000 blocks
      const START_BLOCK = Math.max(0, currentBlock - 20000);
      
      console.log(`Fetching events from block ${START_BLOCK} to ${currentBlock}`);
      
      const tokens = [];
      
      // Process each factory address
      for (const factoryAddress of FACTORY_ADDRESSES) {
        console.log(`Processing factory address: ${factoryAddress}`);
        
        // Query for events from this factory
        const filter = {
          address: factoryAddress,
          topics: [eventTopic],
          fromBlock: START_BLOCK,
          toBlock: currentBlock
        };
        
        try {
          const logs = await provider.getLogs(filter);
          console.log(`Found ${logs.length} token creation events from factory ${factoryAddress}`);
          
          // Process the logs
          for (const log of logs) {
            try {
              // The event parameters are ABI encoded in the data field
              const decodedData = ethers.AbiCoder.defaultAbiCoder().decode(
                ['address', 'uint256', 'address', 'string', 'string', 'uint256', 'address', 'uint256'],
                log.data
              );
              
              const tokenAddress = decodedData[0];
              const deployer = decodedData[2];
              const name = decodedData[3];
              const symbol = decodedData[4];
              const supply = decodedData[5];
              
              console.log(`Found token: ${name} (${symbol}) at ${tokenAddress} from factory ${factoryAddress}`);
              
              // Add to our tokens array
              tokens.push({
                contractAddress: tokenAddress.toLowerCase(),
                name,
                symbol,
                decimals: 18, // ERC20 tokens deployed by factory have 18 decimals
                createdAt: new Date(log.blockNumber * 2000), // Approximate timestamp based on block number (2s per block)
                deployer: deployer.toLowerCase(),
                factory: factoryAddress.toLowerCase() // Track which factory created this token
              });
            } catch (error) {
              console.error(`Error decoding event data:`, error.message);
            }
          }
        } catch (chunkError) {
          console.error(`Error fetching logs for factory ${factoryAddress}:`, chunkError.message);
        }
      }
      
      // Store tokens in database
      if (tokens.length > 0) {
        // Prepare bulk operations
        const operations = tokens.map(token => ({
          updateOne: {
            filter: { contractAddress: token.contractAddress },
            update: { $set: token },
            upsert: true
          }
        }));
        
        // Execute bulk update
        const result = await Token.bulkWrite(operations);
        console.log(`Updated ${result.upsertedCount} new tokens, modified ${result.modifiedCount} existing tokens`);
        console.log(`Total unique tokens found: ${tokens.length}`);
      }
    } catch (error) {
      console.error('Error fetching or decoding logs:', error);
    }
  } catch (error) {
    console.error('Error fetching and storing tokens from factories:', error);
  }
}

// Function to fetch price data for a batch of tokens
async function fetchPriceData(addresses) {
  try {
    // Track API usage
    apiCallsThisMinute++;
    console.log(`API call ${apiCallsThisMinute}/30 this minute`);
    
    // Format addresses for GeckoTerminal (comma-separated, URL encoded)
    const addressesParam = addresses.map(addr => addr.toLowerCase()).join('%2C');
    
    console.log(`Fetching price data for ${addresses.length} tokens from GeckoTerminal`);
    
    // Make the API call to GeckoTerminal
    try {
      const response = await geckoTerminalApi.get(`/networks/base/tokens/multi/${addressesParam}`);
      
      // Format the response to match our API format
      const result = { tokens: {} };
      
      if (response.data && response.data.data) {
        response.data.data.forEach(token => {
          const attributes = token.attributes;
          const address = token.id.split('_')[1].toLowerCase();
          
          result.tokens[address] = {
            price_usd: parseFloat(attributes.price_usd || 0),
            fdv_usd: parseFloat(attributes.fdv_usd || 0),
            volume_usd: parseFloat(attributes.volume_usd?.h24 || 0),
            market_cap: parseFloat(attributes.fdv_usd || 0), // Using FDV as market cap for sorting
            last_updated: new Date(),
            name: attributes.name,
            symbol: attributes.symbol,
            total_reserve_in_usd: parseFloat(attributes.total_reserve_in_usd || 0),
            total_supply: parseFloat(attributes.total_supply || 0),
          };
        });
      }
      
      console.log(`Got price data for ${Object.keys(result.tokens).length} tokens`);
      return result;
    } catch (apiError) {
      console.error('GeckoTerminal API error:', apiError.message);
      return { tokens: {} };
    }
  } catch (error) {
    console.error('Error in price data function:', error.message);
    return { tokens: {} };
  }
}

// Function to update our top tokens list
async function updateTopTokensList() {
  try {
    console.log('Updating top tokens list...');
    
    // Get all tokens with price data, sorted by market cap
    const tokensByMarketCap = await TokenPrice.find({})
      .sort({ market_cap: -1 })
      .limit(PRIORITY_TOKEN_COUNT);
    
    // Update our top tokens list
    topTokens = tokensByMarketCap.map(token => token.contractAddress);
    
    console.log(`Updated top ${PRIORITY_TOKEN_COUNT} tokens: ${topTokens.join(', ')}`);
    lastFullUpdateTime = Date.now();
  } catch (error) {
    console.error('Error updating top tokens list:', error);
  }
}

// Function to fetch and update just the priority tokens
async function updatePriorityTokens() {
  if (apiCallsThisMinute >= 30) {
    console.log('API rate limit reached, skipping priority update');
    return;
  }
  
  if (topTokens.length === 0) {
    console.log('No priority tokens defined yet, running initial update');
    await updateTopTokensList();
    return;
  }
  
  try {
    console.log('Updating priority tokens...');
    
    // Fetch price data for priority tokens
    const priceData = await fetchPriceData(topTokens);
    
    if (priceData && priceData.tokens) {
      // Prepare bulk operations
      const operations = [];
      
      for (const [address, data] of Object.entries(priceData.tokens)) {
        operations.push({
          updateOne: {
            filter: { contractAddress: address },
            update: { 
              $set: {
                ...data,
                priority: true,
                last_updated: new Date()
              }
            },
            upsert: true
          }
        });
      }
      
      // Execute bulk update
      if (operations.length > 0) {
        const result = await TokenPrice.bulkWrite(operations);
        console.log(`Updated price data for ${operations.length} priority tokens`);
      }
    }
  } catch (error) {
    console.error('Error updating priority tokens:', error);
  }
}

// Function to fetch and update non-priority tokens in a rotating fashion
async function updateNonPriorityTokens() {
  if (apiCallsThisMinute >= 28) { // Leave a buffer of 2 calls
    console.log('Near API rate limit, skipping non-priority update');
    return;
  }
  
  try {
    // Get all tokens that are not in the priority list
    const allTokens = await Token.find({
      contractAddress: { $nin: topTokens }
    }, 'contractAddress');
    
    if (allTokens.length === 0) {
      console.log('No non-priority tokens found');
      return;
    }
    
    console.log(`Found ${allTokens.length} non-priority tokens`);
    
    // Calculate how many tokens we can update with remaining API calls
    const remainingCalls = 28 - apiCallsThisMinute; // Leave buffer of 2 calls
    const BATCH_SIZE = 30; // API limit for tokens per call
    const maxBatches = Math.floor(remainingCalls);
    const maxTokens = maxBatches * BATCH_SIZE;
    
    // Get last updated timestamp for cycling through tokens
    const oldestFirstTokens = await TokenPrice.find({
      contractAddress: { $nin: topTokens }
    })
    .sort({ last_updated: 1 })
    .limit(maxTokens)
    .select('contractAddress');
    
    // If we don't have price records yet, use the regular token list
    const tokensToUpdate = oldestFirstTokens.length > 0 
      ? oldestFirstTokens.map(t => t.contractAddress)
      : allTokens.slice(0, maxTokens).map(t => t.contractAddress);
    
    console.log(`Updating ${tokensToUpdate.length} non-priority tokens (oldest first)`);
    
    // Process in batches
    for (let i = 0; i < tokensToUpdate.length; i += BATCH_SIZE) {
      if (apiCallsThisMinute >= 28) break; // Safety check
      
      const batchAddresses = tokensToUpdate.slice(i, i + BATCH_SIZE);
      console.log(`Fetching prices for batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(tokensToUpdate.length/BATCH_SIZE)}`);
      
      try {
        // Fetch price data
        const priceData = await fetchPriceData(batchAddresses);
        
        if (priceData && priceData.tokens) {
          // Prepare bulk operations
          const operations = [];
          
          for (const [address, data] of Object.entries(priceData.tokens)) {
            operations.push({
              updateOne: {
                filter: { contractAddress: address },
                update: { 
                  $set: {
                    ...data,
                    priority: false,
                    last_updated: new Date()
                  }
                },
                upsert: true
              }
            });
          }
          
          // Execute bulk update
          if (operations.length > 0) {
            const result = await TokenPrice.bulkWrite(operations);
            console.log(`Updated price data for ${operations.length} non-priority tokens`);
          }
        }
      } catch (error) {
        console.error(`Error processing batch:`, error);
      }
      
      // Small delay between batches to prevent hammering the API
      if (i + BATCH_SIZE < tokensToUpdate.length) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    
    console.log('Non-priority token update completed');
  } catch (error) {
    console.error('Error updating non-priority tokens:', error);
  }
}

// Periodic full update of all tokens (run less frequently)
async function fullTokenUpdate() {
  try {
    console.log('Running full token update...');
    
    // First update the top tokens list
    await updateTopTokensList();
    
    // Then update all tokens we know about
    await updateNonPriorityTokens();
    
    console.log('Full token update completed');
  } catch (error) {
    console.error('Error in full token update:', error);
  }
}

// Initialize data fetching
async function initializeDataFetching() {
  console.log('Initializing data fetching service...');
  
  // Setup API call tracking
  setupApiCallTracking();
  
  // Immediately fetch tokens on startup
  await fetchAndStoreTokens();
  
  // Run a full update to populate our database and identify top tokens
  await fullTokenUpdate();
  
  // Setup scheduled jobs
  
  // Discover new tokens once per minute
  cron.schedule('*/1 * * * *', fetchAndStoreTokens);
  
  // Update top 10 tokens every 2 seconds (high priority)
  cron.schedule('*/2 * * * * *', updatePriorityTokens);
  
  // Update non-priority tokens every 10 seconds (rotating through them)
  cron.schedule('*/10 * * * * *', updateNonPriorityTokens);
  
  // Run a full update and re-identify top tokens every hour
  cron.schedule('0 * * * *', fullTokenUpdate);
  
  console.log('Data fetching service initialized with prioritized scheduling');
}

// Clean up when shutting down
function shutdown() {
  if (apiCallReset) {
    clearInterval(apiCallReset);
  }
  console.log('Token data service shutdown complete');
}

module.exports = {
  fetchAndStoreTokens,
  updatePriorityTokens,
  updateNonPriorityTokens,
  fullTokenUpdate,
  initializeDataFetching,
  shutdown
};
