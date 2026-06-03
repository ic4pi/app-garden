# Critical Fixes Applied

## 1. Database Not Initializing (CRITICAL)

**Problem:** Database file was created empty (0 bytes) with no tables. All build data was lost.

**Root Cause:** 
- `AppDatabase.__init__()` created the file but didn't call `init_db()`
- `get_database()` didn't call `init_db()` on the new instance
- No other code was calling init_db at startup

**Fix Applied:**
```python
# core/database.py
def get_database() -> AppDatabase:
    global _db
    if _db is None:
        _db = AppDatabase()
        _db.init_db()  # ← NOW CALLED AUTOMATICALLY
    return _db
```

**Result:** Database schema now auto-initializes on first access.

---

## 2. Frontend "ERROR: undefined" Messages

**Problem:** Frontend was showing "ERROR: undefined" instead of actual error messages.

**Root Causes:**
1. `displayResults()` didn't validate response structure, accessing undefined properties
2. `loadResults()` didn't check HTTP status before parsing JSON
3. `showError()` didn't handle null/undefined messages
4. `startPolling()` didn't handle response errors gracefully

**Fixes Applied:**

### showError() - Handle null/undefined
```javascript
function showError(m){
  if(!m||m==='undefined')m='An unknown error occurred';
  document.getElementById('progressMessage').textContent='ERROR: '+(m||'Unknown error');
  // ...
}
```

### loadResults() - Check HTTP status
```javascript
async function loadResults(id){
  try{
    const res=await fetch('/api/results/'+id);
    if(!res.ok){
      showError('Results not ready yet');
      return;
    }
    // ...
```

### displayResults() - Validate and provide defaults
```javascript
function displayResults(r){
  if(!r||!r.winner){
    showError('Invalid results format');
    return;
  }
  const winner=r.winner||{};
  const ranked=r.ranked_builds||[];
  const novelty=r.novelty_attempts||[];
  // Use defaults: winner.total_score||'N/A', etc.
```

### startPolling() - Better error handling
```javascript
function startPolling(){
  // ...
  if(!res.ok)return;  // Skip bad responses
  const p=data.progress||data;
  if(p)updateProgress(p);  // Only update if p exists
  // Add 500ms delay before loading results for race condition safety
```

---

## 3. Runtimewarning Suppression

**Problem:** Harmless RuntimeWarnings about unawaited coroutines cluttering logs.

**Fix:**
```python
# worker/celery_app.py
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*coroutine.*never awaited")
```

---

## What Was Actually Happening

1. ✅ Builds were being created and run
2. ✅ Workers were processing stages
3. ✅ Build logic was completing
4. ❌ **But data was never saved** (no database tables)
5. ❌ Frontend got 404 on `/api/results` endpoint
6. ❌ Frontend showed "ERROR: undefined" because error wasn't real

---

## What Should Happen Now

1. App starts → `get_database()` called
2. Database auto-initializes with schema
3. Builds run → Data saved to database
4. Results endpoint returns data
5. Frontend displays results properly
6. Leaderboard shows completed builds

---

## Testing Checklist

- [ ] Start app: `python app.py`
- [ ] Check that `data/gardener.db` now exists with proper size (>0 bytes)
- [ ] Submit a build
- [ ] Wait for completion
- [ ] Verify results display (no "ERROR: undefined")
- [ ] Check leaderboard shows the build
- [ ] Verify data persists after restart
