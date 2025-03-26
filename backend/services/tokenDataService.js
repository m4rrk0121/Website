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
  '0x9bd7dCc13c532F37F65B0bF078C8f83E037e7445', // Replace with your second factory address
  '0x05Dd3Dc91FAeFAf06499D8D7acecc5a7DecCD4be'  // Replace with your third factory address
];

// Define the TokenCreated event signature
const TOKEN_CREATED_EVENT = 'TokenCreated(address,uint256,address,string,string,uint256,address,uint256)';

// Create a configured axios instance for CoinGecko
const coinGeckoApi = axios.create({
  baseURL: process.env.COINGECKO_API_BASE_URL || 'https://api.coingecko.com/api/v3',
  timeout: 30000,
  headers: {
    'Accept': 'application/json'
  }
});

// Updated fetchAndStoreTokens function to handle multiple factory addresses
async function fetchAndStoreTokens() {
  try {
    console.log('Fetching tokens deployed by multiple factories...');
    
    // Create a simplified RPC provider - let's use Ankr's public endpoint
    const provider = new ethers.JsonRpcProvider(process.env.BASE_RPC_URL || "https://rpc.ankr.com/base");
    
    // Calculate the event topic
    const eventTopic = ethers.id(TOKEN_CREATED_EVENT);
    
    // Get the current block number
    try {
      const currentBlock = await provider.getBlockNumber();
      console.log(`Current block: ${currentBlock}`);
      
      // Focus on the last 50,000 blocks
      const START_BLOCK = Math.max(0, currentBlock - 50000);
      
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

async function fetchPriceData(addresses) {
  try {
    // Format addresses for GeckoTerminal (comma-separated, URL encoded)
    const addressesParam = addresses.map(addr => addr.toLowerCase()).join('%2C');
    
    console.log(`Fetching price data for ${addresses.length} tokens from GeckoTerminal`);
    
    // Create GeckoTerminal API client
    const geckoTerminalApi = axios.create({
      baseURL: 'https://api.geckoterminal.com/api/v2',
      timeout: 30000,
      headers: {
        'Accept': 'application/json'
      }
    });
    
    // Make the API call to GeckoTerminal
    try {
      const response = await geckoTerminalApi.get(`/networks/base/tokens/multi/${addressesParam}`);
      
      // Format the response to match our API format
      const result = { tokens: {} };
      
      if (response.data && response.data.data) {
        response.data.data.forEach(token => {
          const attributes = token.attributes;
          const address = token.id.split('_')[1].toLowerCase();
          
          // Detailed logging of the full token attributes
          console.log(`Full attributes for ${address}:`, JSON.stringify(attributes, null, 2));
          
          result.tokens[address] = {
            price_usd: parseFloat(attributes.price_usd || 0),
            fdv_usd: parseFloat(attributes.fdv_usd || 0),
            // Explicitly handle volume
            volume_usd: parseFloat(attributes.volume_usd.h24 || 0),
            
            last_updated: new Date(),
            name: attributes.name,
            symbol: attributes.symbol,
            
            // Additional data points
            total_reserve_in_usd: parseFloat(attributes.total_reserve_in_usd || 0),
            total_supply: parseFloat(attributes.total_supply || 0),
            
            // Log any missing critical information
            ...(attributes.name ? {} : { missingName: true }),
            ...(attributes.symbol ? {} : { missingSymbol: true })
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

// Function to fetch price data in batches
async function fetchAndStorePrices() {
  try {
    console.log('Fetching price data...');
    
    // Get all tokens from database
    const tokens = await Token.find({}, 'contractAddress');
    const addresses = tokens.map(t => t.contractAddress);
    
    if (addresses.length === 0) {
      console.log('No tokens found to fetch prices for');
      return;
    }
    
    console.log(`Found ${addresses.length} tokens to fetch prices for`);
    
    const BATCH_SIZE = 30; // CoinGecko API limit
    
    // Process in batches
    for (let i = 0; i < addresses.length; i += BATCH_SIZE) {
      const batchAddresses = addresses.slice(i, i + BATCH_SIZE);
      console.log(`Fetching prices for batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(addresses.length/BATCH_SIZE)}`);
      
      try {
        // Fetch price data
        const priceData = await fetchPriceData(batchAddresses);
        
        if (priceData && priceData.tokens) {
          // Prepare bulk operations
          const operations = [];
          
          for (const [address, data] of Object.entries(priceData.tokens)) {
            // Log the data being stored
            console.log(`Storing price data for ${address}:`, JSON.stringify(data, null, 2));
            
            operations.push({
              updateOne: {
                filter: { contractAddress: address },
                update: { 
                  $set: {
                    ...data,
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
            console.log(`Updated price data for ${operations.length} tokens`);
          }
        }
      } catch (error) {
        console.error(`Error processing batch ${Math.floor(i/BATCH_SIZE) + 1}:`, error);
      }
      
      // Respect API rate limits
      if (i + BATCH_SIZE < addresses.length) {
        console.log('Waiting 1 seconds before next batch...');
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    console.log('Price data update completed');
  } catch (error) {
    console.error('Error fetching and storing prices:', error);
  }
}

// Initialize data fetching
async function initializeDataFetching() {
  console.log('Initializing data fetching service...');
  
  // Immediately fetch data on startup
  await fetchAndStoreTokens();
  await fetchAndStorePrices();
  
  // Setup scheduled jobs
  // Fetch tokens every 30 seconds
  cron.schedule('*/30 * * * * *', fetchAndStoreTokens);
  
  // Fetch prices every 30 seconds
  cron.schedule('*/30 * * * * *', fetchAndStorePrices);
  
  console.log('Data fetching service initialized with scheduled jobs');
}

module.exports = {
  fetchAndStoreTokens,
  fetchAndStorePrices,
  initializeDataFetching
};
