# DigiGold Reconciliation Tool

Production-ready web application for reconciling DigiGold transactions across Finfinity, Cashfree, and Augmont systems.

## Features

- **Upload Files**: Supports both **.xlsx** and **.csv** formats for all three files
- **Multiple Sources**: Finfinity, Cashfree, and Augmont transaction exports
- **Automated Reconciliation**: Matches transactions across systems using Order IDs and Merchant Transaction IDs
- **Comprehensive Output**: Single Excel workbook with:
  - Full raw data from all 3 sources
  - Alarmed records (missing from any system)
  - Status-combination sheets for detailed analysis
  - Summary dashboard
- **Production Ready**: Error handling, validation, health checks

## Quick Start (Local Development)

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
python app.py
```

Open http://127.0.0.1:5000 in your browser.

## Production Deployment

### Deploy to Vercel

```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel
```

Or connect your GitHub repository to Vercel for automatic deployments.

## File Requirements

**Supported Formats:** `.xlsx` or `.csv` for all files

### Finfinity File
- `Order Id` - Required
- `Merchant Transaction ID` - Required
- `Order Status` - Optional (for status analysis)

### Cashfree File
- `Order Id` - Required
- `Transaction Status` - Optional (for status analysis)

### Augmont File
- `Merchant Transaction Id` - Required
- `Transaction Status` - Optional (for status analysis)

## Output Structure

The generated `reconciliation_output.xlsx` contains:

1. **FINFINITY** - Complete uploaded data
2. **CASHFREE** - Complete uploaded data
3. **AUGMONT** - Complete uploaded data
4. **ALARMED_RECORDS** - Records missing from Cashfree or Augmont
5. **8 Mismatch Sheets** - Status combination analysis:
   - FIN_SUCCESS_AUG_FAIL_CF_FAIL
   - FIN_FAIL_AUG_PASS_CF_PASS
   - FIN_SUCCESS_AUG_PASS_CF_FAIL
   - FIN_FAIL_AUG_FAIL_CF_PASS
   - FIN_SUCCESS_AUG_FAIL_CF_PASS
   - FIN_FAIL_AUG_FAIL_CF_FAIL
   - FIN_SUCCESS_AUG_PASS_CF_PASS
   - FIN_FAIL_AUG_PASS_CF_FAIL
6. **SUMMARY** - Record counts and metrics

## API Endpoints

- `GET /` - Web interface
- `POST /reconcile` - Upload and process files
- `GET /health` - Health check

## Configuration

Set environment variables:

- `FLASK_ENV=development` - Enable debug mode
- `PORT=5000` - Server port (default: 5000)

## Testing

Generate test data:
```bash
python generate_test_data.py
```

Run tests:
```bash
python test_single_workbook.py
```

## Tech Stack

- **Backend**: Flask (Python)
- **Data Processing**: Pandas
- **Excel**: openpyxl
- **Deployment**: Vercel (serverless)

## Security

- 100MB max file size limit
- File extension validation (.xlsx only)
- Error handling and input validation
- No data persistence (files processed in memory)

## Support

For issues or questions, contact your system administrator.
