import axios from "axios";
import { createObjectCsvWriter } from "csv-writer";
import fs from "fs/promises";
import path from "path";

// Configuration
const CONFIG = {
  API_KEY: process.env.GOOGLE_PLACES_API_KEY,
  BASE_URL: "https://maps.googleapis.com/maps/api/place",
  DEFAULT_RADIUS: 15000,
  DEFAULT_MAX_RESULTS: 200,
  DEFAULT_BATCH_SIZE: 10,
  DELAYS: {
    PAGINATION: 3000,
    BATCH: 1000,
    RETRY: 1000,
    RATE_LIMIT: 5000,
  },
};

// Rate limiting helper
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Utility functions
const createOutputDir = async () => {
  const outputDir = "./output";
  try {
    await fs.access(outputDir);
  } catch {
    await fs.mkdir(outputDir, { recursive: true });
  }
  return outputDir;
};

const logProgress = (current, total, item = "items") => {
  const percentage = ((current / total) * 100).toFixed(1);
  console.log(`   Progress: ${current}/${total} ${item} (${percentage}%)`);
};

async function fetchPlaces(
  query,
  location = "",
  radius = CONFIG.DEFAULT_RADIUS,
  maxResults = CONFIG.DEFAULT_MAX_RESULTS
) {
  let results = [];
  let seenPlaceIds = new Set();
  let url = `${CONFIG.BASE_URL}/textsearch/json?query=${encodeURIComponent(
    query
  )}&radius=${radius}&key=${CONFIG.API_KEY}${
    location ? `&location=${location}` : ""
  }`;

  console.log(`🔍 Starting search for: "${query}"`);
  console.log(
    `📍 Location: ${
      location || "Global"
    } | Radius: ${radius}m | Max Results: ${maxResults}`
  );

  try {
    let pageCount = 0;
    while (url && results.length < maxResults) {
      pageCount++;
      console.log(`📄 Fetching page ${pageCount}...`);

      const response = await axios.get(url);

      // Check for API errors
      if (
        response.data.status !== "OK" &&
        response.data.status !== "ZERO_RESULTS"
      ) {
        console.error(
          `❌ API Error: ${response.data.status} - ${
            response.data.error_message || "Unknown error"
          }`
        );
        if (response.data.status === "REQUEST_DENIED") {
          console.error(
            "🔑 Check your API key and ensure Places API is enabled"
          );
        }
        break;
      }

      if (response.data.results) {
        for (const place of response.data.results) {
          if (
            !seenPlaceIds.has(place.place_id) &&
            results.length < maxResults
          ) {
            results.push(place);
            seenPlaceIds.add(place.place_id);
          }
        }
        console.log(
          `   Found ${response.data.results.length} places (${results.length} total unique)`
        );
      }

      const nextPageToken = response.data.next_page_token;
      if (nextPageToken && results.length < maxResults) {
        console.log("   ⏳ Waiting for next page token to become active...");
        await delay(CONFIG.DELAYS.PAGINATION);
        url = `${CONFIG.BASE_URL}/textsearch/json?pagetoken=${nextPageToken}&key=${CONFIG.API_KEY}`;
      } else {
        url = null;
      }
    }
  } catch (err) {
    console.error(
      "❌ Error fetching places:",
      err.response?.data || err.message
    );
    if (err.response?.status === 429) {
      console.error(
        "🚫 Rate limit exceeded. Consider adding delays or reducing batch size."
      );
    }
  }

  return results;
}

async function fetchPlaceDetails(place_id, retries = 3) {
  const fields = [
    "name",
    "formatted_address",
    "formatted_phone_number",
    "website",
    "rating",
    "user_ratings_total",
    "business_status",
    "opening_hours",
    "price_level",
    "types",
    "vicinity",
  ].join(",");

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const detailsUrl = `${CONFIG.BASE_URL}/details/json?place_id=${place_id}&fields=${fields}&key=${CONFIG.API_KEY}`;
      const response = await axios.get(detailsUrl);

      if (response.data.status === "OK") {
        return response.data.result;
      } else if (response.data.status === "OVER_QUERY_LIMIT") {
        console.warn(
          `⚠️  Query limit reached. Waiting before retry ${attempt}/${retries}...`
        );
        await delay(CONFIG.DELAYS.RATE_LIMIT);
      } else {
        console.warn(
          `⚠️  Place details error for ${place_id}: ${response.data.status}`
        );
        return {};
      }
    } catch (err) {
      console.error(
        `❌ Error fetching place details (attempt ${attempt}/${retries}):`,
        err.message
      );
      if (attempt === retries) return {};
      await delay(CONFIG.DELAYS.RETRY * attempt);
    }
  }
  return {};
}

async function getCarDealers(
  query,
  location = "",
  radius = CONFIG.DEFAULT_RADIUS,
  options = {}
) {
  const {
    maxResults = CONFIG.DEFAULT_MAX_RESULTS,
    outputFile = "car_dealers.csv",
    includeClosedBusinesses = false,
    minRating = 0,
    maxRating = 5,
    batchSize = CONFIG.DEFAULT_BATCH_SIZE,
    requirePhone = false,
    requireWebsite = false,
  } = options;

  console.log("🚗 Car Dealer Lead Generator Starting...");
  console.log("🔎 Search parameters:", {
    query,
    location: location || "Global",
    radius: `${radius}m`,
    maxResults,
    minRating: minRating > 0 ? minRating : "Any",
    requirePhone,
    requireWebsite,
  });

  // Create output directory
  const outputDir = await createOutputDir();
  const fullOutputPath = path.join(outputDir, outputFile);

  const places = await fetchPlaces(query, location, radius, maxResults);
  console.log(`✅ Found ${places.length} unique places`);

  if (places.length === 0) {
    console.log(
      "❌ No places found. Try adjusting your search query or location."
    );
    return;
  }

  const leads = [];
  console.log("🔍 Fetching detailed information...");

  // Process in batches to avoid overwhelming the API
  for (let i = 0; i < places.length; i += batchSize) {
    const batch = places.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(places.length / batchSize);

    console.log(
      `📊 Processing batch ${batchNum}/${totalBatches} (${batch.length} places)`
    );

    const batchPromises = batch.map(async (place, index) => {
      const details = await fetchPlaceDetails(place.place_id);

      // Apply filters
      if (
        !includeClosedBusinesses &&
        details.business_status === "CLOSED_PERMANENTLY"
      ) {
        return null;
      }

      if (
        details.rating &&
        (details.rating < minRating || details.rating > maxRating)
      ) {
        return null;
      }

      if (requirePhone && !details.formatted_phone_number) {
        return null;
      }

      if (requireWebsite && !details.website) {
        return null;
      }

      // Show progress within batch
      if ((index + 1) % 5 === 0 || index === batch.length - 1) {
        logProgress(index + 1, batch.length, "details");
      }

      return {
        name: details.name || "N/A",
        address: details.formatted_address || details.vicinity || "N/A",
        phone: details.formatted_phone_number || "N/A",
        website: details.website || "N/A",
        rating: details.rating || "N/A",
        total_reviews: details.user_ratings_total || 0,
        business_status: details.business_status || "N/A",
        price_level: details.price_level || "N/A",
        types: details.types ? details.types.join(", ") : "N/A",
        place_id: place.place_id,
        search_query: query,
        scraped_at: new Date().toISOString(),
      };
    });

    const batchResults = await Promise.all(batchPromises);
    const validResults = batchResults.filter((result) => result !== null);
    leads.push(...validResults);

    console.log(
      `   ✅ Batch ${batchNum} complete: ${validResults.length} valid leads added`
    );

    // Small delay between batches
    if (i + batchSize < places.length) {
      await delay(CONFIG.DELAYS.BATCH);
    }
  }

  console.log(
    `📈 Processed ${leads.length} valid leads (filtered from ${places.length} places)`
  );

  if (leads.length === 0) {
    console.log("❌ No leads match your criteria. Try adjusting your filters.");
    return;
  }

  // Save to CSV
  const csvWriter = createObjectCsvWriter({
    path: fullOutputPath,
    header: [
      { id: "name", title: "Business Name" },
      { id: "address", title: "Address" },
      { id: "phone", title: "Phone Number" },
      { id: "website", title: "Website" },
      { id: "rating", title: "Google Rating" },
      { id: "total_reviews", title: "Total Reviews" },
      { id: "business_status", title: "Business Status" },
      { id: "price_level", title: "Price Level" },
      { id: "types", title: "Business Types" },
      { id: "place_id", title: "Google Place ID" },
      { id: "search_query", title: "Search Query" },
      { id: "scraped_at", title: "Scraped At" },
    ],
  });

  try {
    await csvWriter.writeRecords(leads);
    console.log(
      `✅ Successfully saved ${leads.length} car dealers to ${fullOutputPath}`
    );

    // Generate summary stats
    const validRatings = leads.filter((lead) => lead.rating !== "N/A");
    const avgRating =
      validRatings.length > 0
        ? validRatings.reduce((sum, lead) => sum + parseFloat(lead.rating), 0) /
          validRatings.length
        : 0;

    const withWebsites = leads.filter((lead) => lead.website !== "N/A").length;
    const withPhones = leads.filter((lead) => lead.phone !== "N/A").length;
    const activeBusinesses = leads.filter(
      (lead) => lead.business_status === "OPERATIONAL"
    ).length;

    console.log("\n📊 Summary Statistics:");
    console.log(`   • Total leads: ${leads.length}`);
    console.log(
      `   • Average rating: ${avgRating ? avgRating.toFixed(1) : "N/A"} (${
        validRatings.length
      } rated)`
    );
    console.log(
      `   • With websites: ${withWebsites} (${(
        (withWebsites / leads.length) *
        100
      ).toFixed(1)}%)`
    );
    console.log(
      `   • With phone numbers: ${withPhones} (${(
        (withPhones / leads.length) *
        100
      ).toFixed(1)}%)`
    );
    console.log(
      `   • Active businesses: ${activeBusinesses} (${(
        (activeBusinesses / leads.length) *
        100
      ).toFixed(1)}%)`
    );
    console.log(`   • Output file: ${fullOutputPath}`);

    return leads;
  } catch (err) {
    console.error("❌ Error writing CSV file:", err.message);
  }
}

// Predefined search configurations
const SEARCH_PRESETS = {
  "used-cars-ny": {
    query: "used car dealers",
    location: "40.7831,-73.9712", // New York City
    radius: 25000,
  },
  "luxury-cars-la": {
    query: "luxury car dealership",
    location: "34.0522,-118.2437", // Los Angeles
    radius: 30000,
  },
  "bmw-dealers-chicago": {
    query: "BMW dealership",
    location: "41.8781,-87.6298", // Chicago
    radius: 20000,
  },
};

// Enhanced usage examples
async function main() {
  try {
    console.log("🚀 Starting Car Dealer Lead Generation");

    // Example 1: Basic search - New York used car dealers
    await getCarDealers(
      "used car dealers in New York",
      "40.7831,-73.9712",
      15000,
      {
        maxResults: 50,
        outputFile: "ny_used_car_dealers.csv",
        minRating: 3.0,
        batchSize: 5,
      }
    );

    // Example 2: Advanced search - High-quality BMW dealers in LA
    // await getCarDealers("BMW dealership", "34.0522,-118.2437", 25000, {
    //   maxResults: 30,
    //   outputFile: "la_bmw_dealers.csv",
    //   minRating: 4.0,
    //   requirePhone: true,
    //   requireWebsite: true,
    //   includeClosedBusinesses: false,
    //   batchSize: 3
    // });

    // Example 3: Using preset configuration
    // const preset = SEARCH_PRESETS['luxury-cars-la'];
    // await getCarDealers(preset.query, preset.location, preset.radius, {
    //   outputFile: "luxury_dealers_la.csv",
    //   minRating: 4.5
    // });
  } catch (error) {
    console.error("❌ Application error:", error.message);
    process.exit(1);
  }
}

// Command line argument parsing
if (process.argv.length > 2) {
  const [, , query, location, radius] = process.argv;
  getCarDealers(
    query || "car dealers",
    location || "",
    parseInt(radius) || 15000
  );
} else {
  // Run the application with default parameters
  if (import.meta.url === `file://${process.argv[1]}`) {
    main();
  }
}

export { getCarDealers, fetchPlaces, fetchPlaceDetails, SEARCH_PRESETS };
