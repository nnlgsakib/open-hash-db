package pages

var DownloadPage = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OpenHashDB - Content View</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --primary-gradient: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%);
            --secondary-gradient: linear-gradient(135deg, #f093fb 0%%, #f5576c 100%%);
            --glass-bg: rgba(255, 255, 255, 0.1);
            --glass-border: rgba(255, 255, 255, 0.2);
            --text-primary: #ffffff;
            --text-secondary: rgba(255, 255, 255, 0.8);
            --shadow-soft: 0 20px 40px rgba(0, 0, 0, 0.1);
            --shadow-elevated: 0 30px 60px rgba(0, 0, 0, 0.2);
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            background: var(--primary-gradient);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: hidden;
            position: relative;
        }

        body::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: 
                radial-gradient(circle at 20%% 80%%, rgba(120, 119, 198, 0.3) 0%%, transparent 50%%),
                radial-gradient(circle at 80%% 20%%, rgba(255, 119, 198, 0.3) 0%%, transparent 50%%),
                radial-gradient(circle at 40%% 40%%, rgba(120, 219, 255, 0.2) 0%%, transparent 50%%);
            animation: gradientShift 20s ease-in-out infinite;
        }

        @keyframes gradientShift {
            0%%, 100%% { opacity: 1; }
            50%% { opacity: 0.8; }
        }

        .floating-elements {
            position: absolute;
            width: 100%%;
            height: 100%%;
            overflow: hidden;
            pointer-events: none;
        }

        .floating-elements::before,
        .floating-elements::after {
            content: '';
            position: absolute;
            width: 300px;
            height: 300px;
            background: linear-gradient(45deg, rgba(255, 255, 255, 0.1), rgba(255, 255, 255, 0.05));
            border-radius: 50%%;
            animation: float 15s ease-in-out infinite;
        }

        .floating-elements::before {
            top: -150px;
            left: -150px;
            animation-delay: 0s;
        }

        .floating-elements::after {
            bottom: -150px;
            right: -150px;
            animation-delay: 7s;
        }

        @keyframes float {
            0%%, 100%% { transform: translateY(0) rotate(0deg); }
            50%% { transform: translateY(-20px) rotate(180deg); }
        }

        .container {
            position: relative;
            z-index: 10;
            max-width: 480px;
            width: 90%%;
            margin: 0 auto;
            padding: 40px;
            background: var(--glass-bg);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid var(--glass-border);
            border-radius: 24px;
            box-shadow: var(--shadow-elevated);
            text-align: center;
            animation: slideUp 0.8s cubic-bezier(0.16, 1, 0.3, 1);
        }

        @keyframes slideUp {
            from {
                opacity: 0;
                transform: translateY(30px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        .icon-container {
            width: 80px;
            height: 80px;
            margin: 0 auto 24px;
            background: linear-gradient(135deg, rgba(255, 255, 255, 0.2), rgba(255, 255, 255, 0.1));
            border-radius: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: var(--shadow-soft);
            position: relative;
            overflow: hidden;
            animation: pulse 2s ease-in-out infinite;
        }

        @keyframes pulse {
            0%%, 100%% { transform: scale(1); }
            50%% { transform: scale(1.05); }
        }

        .icon-container::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%%;
            width: 100%%;
            height: 100%%;
            background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
            animation: shine 3s ease-in-out infinite;
        }

        @keyframes shine {
            0%% { left: -100%%; }
            100%% { left: 100%%; }
        }

        .download-icon {
            width: 36px;
            height: 36px;
            fill: var(--text-primary);
            position: relative;
            z-index: 2;
        }

        h2 {
            font-size: 28px;
            font-weight: 700;
            color: var(--text-primary);
            margin-bottom: 16px;
            letter-spacing: -0.5px;
            line-height: 1.2;
        }

        .subtitle {
            font-size: 16px;
            color: var(--text-secondary);
            margin-bottom: 32px;
            font-weight: 500;
            line-height: 1.5;
        }

        .file-info {
            background: rgba(255, 255, 255, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.12);
            border-radius: 16px;
            padding: 20px;
            margin-bottom: 32px;
            backdrop-filter: blur(10px);
        }

        .hash-display {
            font-family: 'SF Mono', Monaco, Consolas, monospace;
            font-size: 13px;
            color: var(--text-primary);
            background: rgba(255, 255, 255, 0.1);
            padding: 12px 16px;
            border-radius: 10px;
            margin-bottom: 12px;
            word-break: break-all;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .filename {
            font-size: 15px;
            color: var(--text-secondary);
            font-weight: 500;
        }

        .download-btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
            padding: 16px 32px;
            background: var(--secondary-gradient);
            color: var(--text-primary);
            text-decoration: none;
            border-radius: 16px;
            font-weight: 600;
            font-size: 16px;
            box-shadow: var(--shadow-soft);
            transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
            position: relative;
            overflow: hidden;
            border: 1px solid rgba(255, 255, 255, 0.2);
            letter-spacing: 0.3px;
        }

        .download-btn::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(45deg, rgba(255, 255, 255, 0.1), transparent);
            opacity: 0;
            transition: opacity 0.3s ease;
        }

        .download-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.2);
        }

        .download-btn:hover::before {
            opacity: 1;
        }

        .download-btn:active {
            transform: translateY(0);
            box-shadow: var(--shadow-soft);
        }

        .btn-icon {
            width: 20px;
            height: 20px;
            fill: currentColor;
        }

        @media (max-width: 480px) {
            .container {
                width: 95%%;
                padding: 32px 24px;
                border-radius: 20px;
            }
            
            h2 {
                font-size: 24px;
            }
            
            .subtitle {
                font-size: 15px;
            }
            
            .download-btn {
                padding: 14px 24px;
                font-size: 15px;
            }
        }

        @media (prefers-reduced-motion: reduce) {
            * {
                animation-duration: 0.01ms !important;
                animation-iteration-count: 1 !important;
                transition-duration: 0.01ms !important;
            }
        }
    </style>
</head>
<body>
    <div class="floating-elements"></div>
    
    <div class="container">
        <div class="icon-container">
            <svg class="download-icon" viewBox="0 0 24 24">
                <path d="M12 16l-6-6h4V4h4v6h4l-6 6zm-6 2h12v2H6v-2z"/>
            </svg>
        </div>
        
        <h2>Content Ready</h2>
        <p class="subtitle">This file cannot be displayed directly in your browser, but it's ready for download.</p>
        
        <div class="file-info">
            <div class="hash-display">%s</div>
            <div class="filename">%s</div>
        </div>
        
        <a href="/download/%s" download="%s" class="download-btn">
            <svg class="btn-icon" viewBox="0 0 24 24">
                <path d="M12 16l-6-6h4V4h4v6h4l-6 6zm-6 2h12v2H6v-2z"/>
            </svg>
            Download File
        </a>
    </div>
</body>
</html>`
