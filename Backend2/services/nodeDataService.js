const { ethers } = require('ethers');
const cron = require('node-cron');
const mongoose = require('mongoose');
const Token = require('../models/Token');
const GeckoData = require('../models/GeckoData');

// Place it at the top level of your module
let isUpdating = false;

// Configuration
const RPC_URL = process.env.BASE_RPC_URL || 'https://mainnet.base.org';
const UPDATE_INTERVAL = process.env.UPDATE_INTERVAL || '*/30 * * * *';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 30;
const MULTICALL_ADDRESS = '0xcA11bde05977b3631167028862bE2a173976CA11'; // Base Multicall3 address

// USDC on Base - a common stablecoin to use for price reference
const USDC_ADDRESS = ethers.getAddress('0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913');

// Only search for WETH pairs
const WETH_ADDRESS = ethers.getAddress('0x4200000000000000000000000000000000000006'); // WETH on Base

// WETH/USDC Pool on Base to use for pricing
const WETH_USDC_POOL_ADDRESS = ethers.getAddress('0x4C36388bE6F416A29C8d8Eee81C771cE6bE14B18');

// Simplified ERC20 ABI for the functions we need
const ERC20_ABI = [
  'function name() view returns (string)',
  'function symbol() view returns (string)',
  'function decimals() view returns (uint8)',
  'function totalSupply() view returns (uint256)',
  'function balanceOf(address owner) view returns (uint256)'
];

// Uniswap V3 Pool ABI (minimal)
const UNISWAP_V3_POOL_ABI = [
  'function token0() view returns (address)',
  'function token1() view returns (address)',
  'function fee() view returns (uint24)',
  'function liquidity() view returns (uint128)',
  'function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)'
];

// Uniswap V3 Factory ABI
const UNISWAP_V3_FACTORY_ABI = [
  'function getPool(address tokenA, address tokenB, uint24 fee) view returns (address pool)'
];

// Multicall3 ABI fragment we need
const MULTICALL_ABI = [
  'function aggregate3(tuple(address target, bool allowFailure, bytes callData)[] calls) view returns (tuple(bool success, bytes returnData)[])'
];

// Uniswap V3 Factory addresses on Base
const UNISWAP_V3_FACTORY_ADDRESS = '0x33128a8fC17869897dcE68Ed026d694621f6FDfD';

// Common fee tiers in Uniswap V3
const UNISWAP_FEE_TIERS = [100, 500, 3000, 10000];

// Cache the WETH price to avoid excessive RPC calls
let cachedWethPrice = null;
let wethPriceLastUpdated = 0;
const WETH_PRICE_CACHE_TTL = 15 * 60 * 1000; // 15 minutes

// Setup provider
let provider;
try {
  provider = new ethers.JsonRpcProvider(RPC_URL);
  console.log('Connected to Base RPC provider');
} catch (error) {
  console.error('Failed to connect to RPC provider:', error);
  process.exit(1);
}

// Setup Multicall contract
const multicallContract = new ethers.Contract(MULTICALL_ADDRESS, MULTICALL_ABI, provider);

// Setup Uniswap Factory contract
const uniswapFactoryContract = new ethers.Contract(UNISWAP_V3_FACTORY_ADDRESS, UNISWAP_V3_FACTORY_ABI, provider);

/**
 * Fetch the current WETH price from a WETH/USDC pool
 * @returns {Promise<number>} WETH price in USD
 */
async function fetchWethPrice() {
  try {
    // Check if we have a recent cached price
    const now = Date.now();
    if (cachedWethPrice && now - wethPriceLastUpdated < WETH_PRICE_CACHE_TTL) {
      console.log(`Using cached WETH price: $${cachedWethPrice}`);
      return cachedWethPrice;
    }

    console.log('Fetching current WETH price from WETH/USDC pool...');
    
    // Create pool contract instance
    const poolContract = new ethers.Contract(WETH_USDC_POOL_ADDRESS, UNISWAP_V3_POOL_ABI, provider);
    
    // Get token addresses and slot0 data
    const [token0, token1, slot0] = await Promise.all([
      poolContract.token0(),
      poolContract.token1(),
      poolContract.slot0()
    ]);
    
    const tick = Number(slot0[1]);
    const isWethToken0 = token0.toLowerCase() === WETH_ADDRESS.toLowerCase();
    
    console.log(`Debug - token0: ${token0}, token1: ${token1}`);
    console.log(`Debug - tick: ${tick}`);
    
    // For this specific pool, knowing the tick value correlates to a specific price
    // Let's directly calculate the price using the tick value
    
    // From the provided debug logs:
    // At tick -200768, the price should be around 1911
    // Let's use this reference point to establish the correct formula
    
    // The formula we need is:
    // price = 1.0001^tick * constant_factor
    
    const base = 1.0001;
    const tickPower = Math.pow(base, tick);
    
    // From tick: -200768 → price: 1911
    // We can calculate the constant_factor
    // 1911 = 1.0001^(-200768) * constant_factor
    // constant_factor = 1911 / 1.0001^(-200768)
    
    const referenceTick = -200768;
    const referencePrice = 1911;
    const referencePower = Math.pow(base, referenceTick);
    const constantFactor = referencePrice / referencePower;
    
    // Calculate the current price using this formula
    const price = tickPower * constantFactor;
    
    console.log(`Debug - tick power (1.0001^tick): ${tickPower}`);
    console.log(`Debug - constant factor: ${constantFactor}`);
    console.log(`Debug - calculated price: ${price}`);
    
    // Cache the result
    cachedWethPrice = price;
    wethPriceLastUpdated = now;
    
    console.log(`Current WETH price: $${price}`);
    return price;
  } catch (error) {
    console.error('Error fetching WETH price:', error);
    return cachedWethPrice || 1911; // Fallback to a reasonable value if fetch fails
  }
}

// Helper function to encode function calls for multicall
function encodeFunctionCall(contractInterface, functionName, params = []) {
  return contractInterface.encodeFunctionData(functionName, params);
}

// Helper to decode return data
function decodeFunctionResult(contractInterface, functionName, data) {
  return contractInterface.decodeFunctionResult(functionName, data);
}

// Helper function to chunk array into batches
function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

// Get basic token information using multicall
async function getTokensBasicInfo(tokenAddresses) {
  try {
    console.log(`Fetching basic info for ${tokenAddresses.length} tokens via node...`);
    
    const erc20Interface = new ethers.Interface(ERC20_ABI);
    const calls = [];
    
    // Create calls for each token and each function
    for (const address of tokenAddresses) {
      // Add name call
      calls.push({
        target: address,
        allowFailure: true,
        callData: encodeFunctionCall(erc20Interface, 'name')
      });
      
      // Add symbol call
      calls.push({
        target: address,
        allowFailure: true,
        callData: encodeFunctionCall(erc20Interface, 'symbol')
      });
      
      // Add decimals call
      calls.push({
        target: address,
        allowFailure: true,
        callData: encodeFunctionCall(erc20Interface, 'decimals')
      });
      
      // Add totalSupply call
      calls.push({
        target: address,
        allowFailure: true,
        callData: encodeFunctionCall(erc20Interface, 'totalSupply')
      });
    }
    
    // Make multicall
    const results = await multicallContract.aggregate3(calls);
    
    // Process results
    const tokenInfoMap = {};
    let currentToken = 0;
    
    for (let i = 0; i < results.length; i += 4) {
      const address = tokenAddresses[currentToken].toLowerCase();
      
      // Extract data
      let name = '';
      let symbol = '';
      let decimals = 18;
      let totalSupply = '0';
      
      try {
        if (results[i].success) {
          name = decodeFunctionResult(erc20Interface, 'name', results[i].returnData)[0];
        }
        
        if (results[i+1].success) {
          symbol = decodeFunctionResult(erc20Interface, 'symbol', results[i+1].returnData)[0];
        }
        
        if (results[i+2].success) {
          decimals = decodeFunctionResult(erc20Interface, 'decimals', results[i+2].returnData)[0];
        }
        
        if (results[i+3].success) {
          totalSupply = decodeFunctionResult(erc20Interface, 'totalSupply', results[i+3].returnData)[0].toString();
        }
      } catch (error) {
        console.error(`Error decoding results for token ${address}:`, error);
      }
      
      tokenInfoMap[address] = {
        address,
        name,
        symbol,
        decimals: Number(decimals),
        totalSupply,
        pools: []
      };
      
      currentToken++;
    }
    
    return tokenInfoMap;
  } catch (error) {
    console.error('Error getting tokens basic info:', error);
    return {};
  }
}

// Find WETH pools for tokens using batched multicall
async function findWethPoolsBatched(tokenAddresses) {
  try {
    console.log(`Finding WETH pools for ${tokenAddresses.length} tokens using batched calls...`);
    
    const tokenPoolsMap = {};
    const poolDataMap = {};
    
    // Initialize pool arrays for each token
    for (const address of tokenAddresses) {
      tokenPoolsMap[address.toLowerCase()] = [];
    }
    
    // Prepare the batch calls
    const uniswapFactoryInterface = new ethers.Interface(UNISWAP_V3_FACTORY_ABI);
    const calls = [];
    const callMetadata = []; // Store what each call is for
    
    // Create calls for all token-WETH-fee combinations in both directions
    for (const tokenAddress of tokenAddresses) {
      const token = tokenAddress.toLowerCase();
      const checksumToken = ethers.getAddress(token);
      
      for (const fee of UNISWAP_FEE_TIERS) {
        // Token as token0, WETH as token1
        calls.push({
          target: UNISWAP_V3_FACTORY_ADDRESS,
          allowFailure: true,
          callData: encodeFunctionCall(
            uniswapFactoryInterface, 
            'getPool', 
            [checksumToken, WETH_ADDRESS, fee]
          )
        });
        callMetadata.push({
          tokenAddress: token,
          token0: token,
          token1: WETH_ADDRESS.toLowerCase(),
          fee: fee
        });
        
        // WETH as token0, token as token1 (reverse order)
        calls.push({
          target: UNISWAP_V3_FACTORY_ADDRESS,
          allowFailure: true,
          callData: encodeFunctionCall(
            uniswapFactoryInterface, 
            'getPool', 
            [WETH_ADDRESS, checksumToken, fee]
          )
        });
        callMetadata.push({
          tokenAddress: token,
          token0: WETH_ADDRESS.toLowerCase(),
          token1: token,
          fee: fee
        });
      }
    }
    
    console.log(`Making ${calls.length} batched calls to find WETH pools...`);
    
    // Execute multicall in batches (to avoid exceeding gas limits)
    const MULTICALL_BATCH_SIZE = 2000; // Adjust based on RPC provider limits
    for (let i = 0; i < calls.length; i += MULTICALL_BATCH_SIZE) {
      const batchCalls = calls.slice(i, i + MULTICALL_BATCH_SIZE);
      const batchMetadata = callMetadata.slice(i, i + MULTICALL_BATCH_SIZE);
      
      console.log(`Processing batch ${Math.floor(i/MULTICALL_BATCH_SIZE) + 1} of ${Math.ceil(calls.length/MULTICALL_BATCH_SIZE)}...`);
      
      // Execute multicall
      const results = await multicallContract.aggregate3(batchCalls);
      
      // Process results from this batch
      for (let j = 0; j < results.length; j++) {
        const result = results[j];
        const metadata = batchMetadata[j];
        
        if (result.success) {
          try {
            // Decode the pool address
            const poolAddress = decodeFunctionResult(
              uniswapFactoryInterface, 
              'getPool', 
              result.returnData
            )[0];
            
            // If pool exists (not zero address)
            if (poolAddress !== ethers.ZeroAddress) {
              const token = metadata.tokenAddress;
              
              console.log(`Found pool ${poolAddress} for ${metadata.token0} - ${metadata.token1} with fee ${metadata.fee}`);
              
              // Add to token's pools if not already added
              if (!tokenPoolsMap[token].includes(poolAddress)) {
                tokenPoolsMap[token].push(poolAddress);
                
                // Store pool data for later processing
                poolDataMap[poolAddress] = {
                  address: poolAddress,
                  token0: metadata.token0,
                  token1: metadata.token1,
                  fee: metadata.fee,
                  dex: 'Uniswap V3'
                };
              }
            }
          } catch (error) {
            console.error(`Error decoding pool result: ${error.message}`);
          }
        }
      }
      
      // Only add delay between batches if not the last batch
      if (i + MULTICALL_BATCH_SIZE < calls.length) {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
    
    // Log summary for all tokens
    for (const token of tokenAddresses) {
      const lowercaseToken = token.toLowerCase();
      const poolCount = tokenPoolsMap[lowercaseToken].length;
      if (poolCount > 0) {
        console.log(`Found ${poolCount} WETH pools for ${lowercaseToken}`);
      } else {
        console.log(`No WETH pools found for ${lowercaseToken}`);
      }
    }
    
    return { tokenPoolsMap, poolDataMap };
  } catch (error) {
    console.error('Error finding WETH pools:', error);
    return { tokenPoolsMap: {}, poolDataMap: {} };
  }
}

// Get pool data using multicall
async function getPoolsData(poolAddresses, poolDataMap) {
  try {
    if (poolAddresses.length === 0) {
      return poolDataMap;
    }
    
    console.log(`Fetching data for ${poolAddresses.length} pools...`);
    
    const poolInterface = new ethers.Interface(UNISWAP_V3_POOL_ABI);
    const calls = [];
    
    // Create calls for each pool
    for (const poolAddress of poolAddresses) {
      // Get token0
      calls.push({
        target: poolAddress,
        allowFailure: true,
        callData: encodeFunctionCall(poolInterface, 'token0')
      });
      
      // Get token1
      calls.push({
        target: poolAddress,
        allowFailure: true,
        callData: encodeFunctionCall(poolInterface, 'token1')
      });
      
      // Get fee
      calls.push({
        target: poolAddress,
        allowFailure: true,
        callData: encodeFunctionCall(poolInterface, 'fee')
      });
      
      // Get liquidity
      calls.push({
        target: poolAddress,
        allowFailure: true,
        callData: encodeFunctionCall(poolInterface, 'liquidity')
      });
      
      // Get slot0 (contains price and tick info)
      calls.push({
        target: poolAddress,
        allowFailure: true,
        callData: encodeFunctionCall(poolInterface, 'slot0')
      });
    }
    
    // Make multicall
    const results = await multicallContract.aggregate3(calls);
    
    // Process results
    let currentPool = 0;
    
    for (let i = 0; i < results.length; i += 5) {
      const poolAddress = poolAddresses[currentPool];
      
      try {
        if (results[i].success && poolDataMap[poolAddress]) {
          poolDataMap[poolAddress].token0 = decodeFunctionResult(poolInterface, 'token0', results[i].returnData)[0].toLowerCase();
        }
        
        if (results[i+1].success && poolDataMap[poolAddress]) {
          poolDataMap[poolAddress].token1 = decodeFunctionResult(poolInterface, 'token1', results[i+1].returnData)[0].toLowerCase();
        }
        
        if (results[i+2].success && poolDataMap[poolAddress]) {
          poolDataMap[poolAddress].fee = decodeFunctionResult(poolInterface, 'fee', results[i+2].returnData)[0];
        }
        
        if (results[i+3].success && poolDataMap[poolAddress]) {
          poolDataMap[poolAddress].liquidity = decodeFunctionResult(poolInterface, 'liquidity', results[i+3].returnData)[0].toString();
        }
        
        if (results[i+4].success && poolDataMap[poolAddress]) {
          const slot0 = decodeFunctionResult(poolInterface, 'slot0', results[i+4].returnData);
          poolDataMap[poolAddress].sqrtPriceX96 = slot0[0].toString();
          poolDataMap[poolAddress].tick = slot0[1];
        }
      } catch (error) {
        console.error(`Error processing pool data for ${poolAddress}:`, error);
      }
      
      currentPool++;
    }
    
    return poolDataMap;
  } catch (error) {
    console.error('Error getting pools data:', error);
    return poolDataMap;
  }
}

// Calculate price from tick
function calculatePriceFromTick(tick, decimals0, decimals1) {
  try {
    // price = 1.0001^tick * 10^(decimals1 - decimals0)
    const base = 1.0001;
    const exponent = tick;
    
    // Handle large tick values more accurately
    let price;
    
    if (Math.abs(exponent) > 1000) {
      // For large exponents, use logarithmic calculation for better accuracy
      const logPrice = exponent * Math.log(base);
      price = Math.exp(logPrice);
    } else {
      price = Math.pow(base, exponent);
    }
    
    // Adjust for decimal differences between tokens
    const decimalAdjustment = Math.pow(10, decimals1 - decimals0);
    
    return price * decimalAdjustment;
  } catch (error) {
    console.error('Error calculating price from tick:', error);
    return 0;
  }
}

// Calculate market cap more accurately
function calculateMarketCap(totalSupply, decimals, price) {
  try {
    // Handle large numbers more precisely
    if (!totalSupply || !price) return 0;
    
    // Convert totalSupply to a normalized value
    const divisor = Math.pow(10, decimals);
    
    // For very large total supplies, use BigInt for precision
    let normalizedSupply;
    
    if (totalSupply.length > 15) {
      // For very large numbers, calculate in parts using BigInt
      const totalSupplyBigInt = BigInt(totalSupply);
      const divisorBigInt = BigInt(Math.floor(divisor));
      
      // Use BigInt division and convert to Number carefully
      const wholePart = Number(totalSupplyBigInt / divisorBigInt);
      const fractionPart = Number((totalSupplyBigInt % divisorBigInt) / divisorBigInt);
      normalizedSupply = wholePart + fractionPart;
    } else {
      // For smaller numbers, direct division works
      normalizedSupply = Number(totalSupply) / divisor;
    }
    
    return normalizedSupply * price;
  } catch (error) {
    console.error('Error calculating market cap:', error);
    return 0;
  }
}

// Calculate liquidity in USD based on tick and sqrtPriceX96
function calculateLiquidityInUSD(liquidity, sqrtPriceX96, tick, price, isToken0, token0Decimals, token1Decimals, wethPriceUsd) {
  try {
    if (!liquidity || liquidity === '0' || !price) {
      return 0;
    }
    
    // For concentrated liquidity in Uniswap V3, we need to use a different approach
    // Using the SqrtX96 price from the pool to convert liquidity to amounts

    // Convert liquidity to a manageable number
    const liquidityNum = Number(liquidity);
    
    // Calculate token amounts (approximation)
    let tokenAmount, wethAmount;
    
    if (isToken0) {
      // If our token is token0, then token1 is WETH
      const sqrtPrice = Number(sqrtPriceX96) / (2 ** 96);
      
      // At current price (approximation based on Uniswap V3 formula)
      wethAmount = liquidityNum * sqrtPrice / (10 ** 18); // WETH has 18 decimals
      tokenAmount = liquidityNum / sqrtPrice / (10 ** token0Decimals);
      
      // Total value in USD
      const tokenValueUSD = tokenAmount * price;
      const wethValueUSD = wethAmount * wethPriceUsd; // Use dynamic WETH price
      
      return tokenValueUSD + wethValueUSD;
    } else {
      // If our token is token1, then token0 is WETH
      const sqrtPrice = Number(sqrtPriceX96) / (2 ** 96);
      
      // At current price
      tokenAmount = liquidityNum * sqrtPrice / (10 ** token1Decimals);
      wethAmount = liquidityNum / sqrtPrice / (10 ** 18); // WETH has 18 decimals
      
      // Total value in USD
      const tokenValueUSD = tokenAmount * price;
      const wethValueUSD = wethAmount * wethPriceUsd; // Use dynamic WETH price
      
      return tokenValueUSD + wethValueUSD;
    }
  } catch (error) {
    console.error('Error calculating liquidity in USD:', error);
    return 0;
  }
}

// Process and enrich token data with pool information using tick-based calculations
async function enrichWithPoolData(tokenInfoMap) {
  try {
    console.log('Processing pool data for tokens using tick-based calculations...');
    
    // Fetch the current WETH price at the start
    const wethPriceUsd = await fetchWethPrice();
    console.log(`Using WETH price: $${wethPriceUsd} for calculations`);
    
    // For each token, process its pools
    for (const [tokenAddress, tokenData] of Object.entries(tokenInfoMap)) {
      const pools = tokenData.pools || [];
      
      // Skip tokens with no pools
      if (pools.length === 0) {
        continue;
      }
      
      // Add pool count
      tokenInfoMap[tokenAddress].pool_count = pools.length;
      
      // Calculate price if possible using WETH pools
      let price = 0;
      let liquidity_usd = 0;
      let highestLiquidity = BigInt(0);
      let bestPool = null;
      let mainPoolAddress = '';
      let mainPoolTick = 0;
      
      for (const pool of pools) {
        if (!pool || !pool.liquidity || pool.tick === undefined) {
          continue;
        }
        
        const liquidity = BigInt(pool.liquidity);
        
        // Track pool with highest liquidity
        if (liquidity > highestLiquidity) {
          highestLiquidity = liquidity;
          bestPool = pool;
          mainPoolAddress = pool.address;
          
          // Convert tick to number if it's a BigInt
          mainPoolTick = typeof pool.tick === 'bigint' ? 
            Number(pool.tick) : Number(pool.tick);
            
          tokenInfoMap[tokenAddress].main_dex = pool.dex;
        }
      }
      
      // Store the main pool address and tick
      tokenInfoMap[tokenAddress].main_pool_address = mainPoolAddress;
      tokenInfoMap[tokenAddress].main_pool_tick = mainPoolTick;
      
      // If we found a pool with liquidity, calculate price using tick
      if (bestPool && bestPool.tick !== undefined) {
        // Determine if token is token0 or token1 in the pool
        const isToken0 = bestPool.token0.toLowerCase() === tokenAddress.toLowerCase();
        
        // Get decimals for both tokens
        const token0Decimals = isToken0 ? 
          Number(tokenInfoMap[tokenAddress].decimals) : 18; // WETH has 18 decimals
        const token1Decimals = isToken0 ? 
          18 : Number(tokenInfoMap[tokenAddress].decimals);
        
        // Convert tick to number if it's a BigInt
        const tickNumber = typeof bestPool.tick === 'bigint' ? 
          Number(bestPool.tick) : Number(bestPool.tick);
        
        // Calculate price from tick
        if (isToken0) {
          // If token is token0, price = token1/token0
          const priceInWETH = calculatePriceFromTick(tickNumber, token0Decimals, token1Decimals);
          // Convert to USD using current WETH price
          price = priceInWETH * wethPriceUsd;
        } else {
          // If token is token1, price = 1/(token0/token1)
          const priceInverse = calculatePriceFromTick(tickNumber, token1Decimals, token0Decimals);
          const priceInWETH = 1 / priceInverse;
          // Convert to USD using current WETH price
          price = priceInWETH * wethPriceUsd;
        }
        
        // Calculate liquidity in USD based on tick and price
        liquidity_usd = calculateLiquidityInUSD(
          bestPool.liquidity,
          bestPool.sqrtPriceX96,
          tickNumber,
          price,
          isToken0,
          token0Decimals,
          token1Decimals,
          wethPriceUsd // Pass the current WETH price
        );
      }
      
      // Set price and liquidity in token data
      tokenInfoMap[tokenAddress].price_usd = price;
      tokenInfoMap[tokenAddress].liquidity_usd = liquidity_usd;
      
      // Calculate approximate market cap if price is available
      if (price > 0) {
        tokenInfoMap[tokenAddress].market_cap = calculateMarketCap(
          tokenInfoMap[tokenAddress].totalSupply,
          tokenInfoMap[tokenAddress].decimals,
          price
        );
      }
    }
    
    return tokenInfoMap;
  } catch (error) {
    console.error('Error enriching with pool data:', error);
    return tokenInfoMap;
  }
}

// Process and store token data
// Process and store token data using bulk operations
async function processAndStoreTokensData(tokensData) {
  try {
    if (!tokensData || Object.keys(tokensData).length === 0) {
      console.log('No tokens data to process');
      return [];
    }
    
    console.log(`Processing ${Object.keys(tokensData).length} tokens...`);
    
    // Prepare bulk operations array
    const bulkOperations = [];
    const processedTokens = [];
    
    for (const [address, tokenData] of Object.entries(tokensData)) {
      if (!tokenData) continue;
      
      // Create token document
      const tokenDoc = {
        contractAddress: address.toLowerCase(),
        name: tokenData.name || '',
        symbol: tokenData.symbol || '',
        decimals: tokenData.decimals || 18,
        totalSupply: tokenData.totalSupply || '0',
        pool_count: tokenData.pool_count || 0,
        price_usd: tokenData.price_usd || 0,
        market_cap: tokenData.market_cap || 0,
        main_dex: tokenData.main_dex || '',
        main_pool_address: tokenData.main_pool_address || '',
        main_pool_tick: tokenData.main_pool_tick || 0,
        lastUpdated: new Date()
      };
      
      // Add to bulk operations array
      bulkOperations.push({
        updateOne: {
          filter: { contractAddress: tokenDoc.contractAddress },
          update: { $set: tokenDoc },
          upsert: true
        }
      });
      
      processedTokens.push(tokenDoc);
    }
    
    // Execute bulk write operation if there are operations to perform
    if (bulkOperations.length > 0) {
      const startTime = Date.now();
      
      // Execute bulk operation
      const result = await GeckoData.bulkWrite(bulkOperations);
      
      const duration = Date.now() - startTime;
      console.log(`Bulk database operation completed in ${duration}ms`);
      console.log(`Modified: ${result.modifiedCount}, Upserted: ${result.upsertedCount}`);
    }
    
    console.log(`Successfully processed ${processedTokens.length} tokens`);
    return processedTokens;
  } catch (error) {
    console.error('Error processing token data:', error);
    return [];
  }
}
// Add this function before the module.exports
function initializeDataFetching() {
  console.log('Initializing token data fetching service using direct node connection...');
  
  // Test provider connection and fetch initial WETH price
  Promise.all([
    provider.getBlockNumber(),
    fetchWethPrice() // Pre-fetch WETH price
  ])
    .then(([blockNumber, wethPrice]) => {
      console.log(`Connected to Base chain. Current block: ${blockNumber}`);
      console.log(`Initial WETH price: $${wethPrice}`);
    })
    .catch(error => {
      console.error('Failed to connect to Base chain:', error);
    });
  
  // Run immediately on startup with a small delay
  setTimeout(() => {
    updateTokenData();
  }, 5000);
  
  // Schedule to run every second
   const cronSchedule = `* * * * * *`; // Format is seconds, minutes, hours, day of month, month, day of week
  
  cron.schedule(cronSchedule, () => {
    if (isUpdating) {
      console.log('Update already in progress, skipping...');
      return;
    }
    
    isUpdating = true;
    console.log('Running scheduled token data update...');
    
    updateTokenData()
      .finally(() => {
        isUpdating = false;
      });
  });
  
  console.log(`Token data will be updated according to schedule: ${cronSchedule}`);
}

// Main function to update token data
async function updateTokenData() {
  console.log('Starting token data update...');
  
  try {
    // Get all token contract addresses from the Token collection
    const tokens = await Token.find({}, { contractAddress: 1 });
    console.log(`Found ${tokens.length} tokens to process`);
    
    // Extract all contract addresses
    const addresses = tokens.map(token => token.contractAddress);
    
    // Create batches of addresses for token info
    const batches = chunkArray(addresses, BATCH_SIZE);
    console.log(`Created ${batches.length} batches of max ${BATCH_SIZE} tokens each for basic info`);
    
    let processedCount = 0;
    
    // Process each batch
    for (let i = 0; i < batches.length; i++) {
      console.log(`Processing batch ${i + 1} of ${batches.length}`);
      
      // Get basic info from node
      const tokenBasicInfo = await getTokensBasicInfo(batches[i]);
      
      // Find WETH pools for all tokens in this batch at once using multicall
      const { tokenPoolsMap, poolDataMap } = await findWethPoolsBatched(batches[i]);
      
      // Get data for all unique pool addresses
      const allPoolAddresses = [...new Set(
        Object.values(tokenPoolsMap).flat()
      )];
      
      // Get detailed pool data
      const enrichedPoolDataMap = await getPoolsData(allPoolAddresses, poolDataMap);
      
      // Enrich token data with pool information
      for (const tokenAddress of batches[i]) {
        const token = tokenAddress.toLowerCase();
        
        if (tokenBasicInfo[token]) {
          // Add pools to token data
          tokenBasicInfo[token].pools = (tokenPoolsMap[token] || []).map(
            poolAddress => enrichedPoolDataMap[poolAddress]
          );
          
          // Add pool count
          tokenBasicInfo[token].pool_count = (tokenPoolsMap[token] || []).length;
        }
      }
      
      // Calculate prices and liquidity
      const enrichedData = await enrichWithPoolData(tokenBasicInfo);
      
      // Store in database
      const updatedTokens = await processAndStoreTokensData(enrichedData);
      processedCount += updatedTokens.length;
      
      // Add delay between batches
      if (i + 1 < batches.length) {
        console.log('Waiting between batches...');
        await new Promise(resolve => setTimeout(resolve, 3000));
      }
    }
    
    console.log(`Token data update completed. Processed ${processedCount} out of ${addresses.length} tokens.`);
  } catch (error) {
    console.error('Error updating token data:', error);
  }
}
// Then keep your exports
module.exports = {
  initializeDataFetching,
  updateTokenData,
  getTokensBasicInfo,
  findWethPoolsBatched,
  fetchWethPrice
};
