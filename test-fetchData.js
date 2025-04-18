const { fetchData } = require('./index'); // Import fetchData from index.js

(async () => {
  try {
    const rows = await fetchData('SELECT NOW()');
    console.log('✅ Query successful:', rows);
  } catch (err) {
    console.error('❌ Query failed:', err.message);
  }
})();
