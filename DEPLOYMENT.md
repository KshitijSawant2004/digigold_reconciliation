# DigiGold Reconciliation Tool - Vercel Deployment

## Files Included

- `app.py` - Flask application (main app)
- `requirements.txt` - Python dependencies
- `templates/index.html` - Web interface
- `vercel.json` - Vercel configuration
- `.gitignore` - Git ignore rules

## Deploy to Vercel

### Option 1: GitHub (Recommended)

1. **Push to GitHub**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git push origin main
   ```

2. **Connect to Vercel**
   - Go to vercel.com
   - Click "New Project"
   - Import your GitHub repository
   - Click "Deploy"

### Option 2: Direct Upload

1. Go to vercel.com
2. Click "New Project"
3. Select "Other" or drag folder
4. Click "Deploy"

---

## How It Works

1. Visit your Vercel URL (e.g., https://your-project.vercel.app)
2. Upload 3 Excel files:
   - KS Digi Gold Buy
   - Cashfree Digi Gold
   - Augmont Digi Gold
3. Click "▶ Start Reconciliation"
4. Download the report!

---

## File Requirements

**KS File needs:**
- "Merchant Transaction ID"
- "Order Id"

**Cashfree File needs:**
- "Order Id"

**Augmont File needs:**
- "Merchant Transaction Id"

---

## What You Get

Excel file with 3 sheets:

**Sheet 1: KS Reconciliation**
- All your KS records
- "In Augmont?" → YES or NO
- "In Cashfree?" → YES or NO

**Sheet 2: Missing in KS**
- Transactions in Augmont but NOT in KS
- Transactions in Cashfree but NOT in KS

**Sheet 3: Summary**
- Total counts
- Matches
- Missing items

---

## Vercel Environment

- Python 3.9+
- Serverless functions
- Cold start optimized
- Auto-scaling

---

**Ready to deploy!** 🚀
