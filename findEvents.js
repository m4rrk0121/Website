// findEvents.js - corrected version
const { ethers } = require('ethers');
require('dotenv').config();

async function findFactoryEvents() {
  // Try different RPC providers
  const providers = [
    new ethers.JsonRpcProvider("https://mainnet.base.org"), 
    new ethers.JsonRpcProvider("https://base-mainnet.public.blastapi.io")
  ];
  
  const factoryAddress = '0xb51F74E6d8568119061f59Fd7f98824F1e666AC1';
  
  // Try each provider
  for (const provider of providers) {
    try {
      console.log(`Trying provider URL: ${provider.connection?.url || "unknown"}`);
      
      // Get current block
      const currentBlock = await provider.getBlockNumber();
      
      // Look at a smaller range - just 1000 blocks
      const fromBlock = currentBlock - 1000;
      
      console.log(`Searching for events from block ${fromBlock} to ${currentBlock}`);
      
      // Get all events from this contract (no topic filter)
      const logs = await provider.getLogs({
        address: factoryAddress,
        fromBlock,
        toBlock: currentBlock
      });
      
      console.log(`Found ${logs.length} events`);
      
      if (logs.length > 0) {
        // We found events, process them
        processLogs(logs);
        // If successful, exit the loop
        return;
      }
    } catch (error) {
      console.error(`Error with provider:`, error.message);
      // Continue to next provider
    }
  }
  
  console.log("All providers failed. Consider using an API key from Alchemy or Infura.");
}

function processLogs(logs) {
  // Group by topic0 (event signature)
  const eventGroups = {};
  
  for (const log of logs) {
    const eventHash = log.topics[0];
    
    if (!eventGroups[eventHash]) {
      eventGroups[eventHash] = [];
    }
    
    eventGroups[eventHash].push(log);
  }
  
  // Print summary of events
  console.log("\nEvent signatures found:");
  for (const [hash, events] of Object.entries(eventGroups)) {
    console.log(`${hash}: ${events.length} occurrences`);
    
    // Show sample data
    if (events.length > 0) {
      const sample = events[0];
      console.log("  Topics:", sample.topics);
      console.log("  Data:", sample.data);
      console.log("  Transaction:", sample.transactionHash);
    }
  }
}

findFactoryEvents().catch(console.error);