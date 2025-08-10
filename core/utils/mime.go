package utils

import (
	"path/filepath"
	"strings"
)

func GetMimeType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".txt":
		return "text/plain"
	case ".html", ".htm":
		return "text/html"
	case ".css":
		return "text/css"
	case ".js", ".mjs":
		return "application/javascript"
	case ".json":
		return "application/json"
	case ".jsonld":
		return "application/ld+json"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".svg":
		return "image/svg+xml"
	case ".webp":
		return "image/webp"
	case ".ico":
		return "image/x-icon"
	case ".bmp":
		return "image/bmp"
	case ".avif":
		return "image/avif"
	case ".heif", ".heic":
		return "image/heif"
	case ".tiff", ".tif":
		return "image/tiff"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".ogg":
		return "audio/ogg"
	case ".flac":
		return "audio/flac"
	case ".aac":
		return "audio/aac"
	case ".mp4":
		return "video/mp4"
	case ".webm":
		return "video/webm"
	case ".mpeg", ".mpg":
		return "video/mpeg"
	case ".ogv":
		return "video/ogg"
	case ".avi":
		return "video/x-msvideo"
	case ".mov":
		return "video/quicktime"
	case ".woff":
		return "font/woff"
	case ".woff2":
		return "font/woff2"
	case ".ttf":
		return "font/ttf"
	case ".otf":
		return "font/otf"
	case ".eot":
		return "application/vnd.ms-fontobject"
	case ".xml":
		return "application/xml"
	case ".xhtml":
		return "application/xhtml+xml"
	case ".wasm":
		return "application/wasm"
	case ".csv":
		return "text/csv"
	case ".vtt":
		return "text/vtt"
	case ".md", ".markdown":
		return "text/markdown"
	case ".ts":
		return "video/mp2t"
	case ".m3u8":
		return "application/vnd.apple.mpegurl"

	// Browser-unrenderable MIME types
	case ".pdf":
		return "application/pdf"
	case ".zip":
		return "application/zip"
	case ".rar":
		return "application/x-rar-compressed"
	case ".7z":
		return "application/x-7z-compressed"
	case ".tar":
		return "application/x-tar"
	case ".gz":
		return "application/gzip"
	case ".bz2":
		return "application/x-bzip2"
	case ".xz":
		return "application/x-xz"
	case ".zst":
		return "application/zstd"
	case ".exe":
		return "application/x-msdownload"
	case ".doc":
		return "application/msword"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".xls":
		return "application/vnd.ms-excel"
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".ppt":
		return "application/vnd.ms-powerpoint"
	case ".pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case ".odt":
		return "application/vnd.oasis.opendocument.text"
	case ".ods":
		return "application/vnd.oasis.opendocument.spreadsheet"
	case ".odp":
		return "application/vnd.oasis.opendocument.presentation"
	case ".odg":
		return "application/vnd.oasis.opendocument.graphics"
	case ".rtf":
		return "application/rtf"
	case ".epub":
		return "application/epub+zip"
	case ".jar":
		return "application/java-archive"
	case ".war":
		return "application/x-webarchive"
	case ".bin":
		return "application/octet-stream"
	case ".iso":
		return "application/x-iso9660-image"
	case ".dmg":
		return "application/x-apple-diskimage"
	case ".torrent":
		return "application/x-bittorrent"
	case ".sql":
		return "application/sql"
	case ".db", ".sqlite":
		return "application/x-sqlite3"
	case ".psd":
		return "image/vnd.adobe.photoshop"
	case ".ai":
		return "application/postscript"
	case ".eps":
		return "application/postscript"
	case ".vcf", ".vcard":
		return "text/vcard"
	case ".ics", ".ical":
		return "text/calendar"
	case ".apk":
		return "application/vnd.android.package-archive"
	case ".deb":
		return "application/vnd.debian.binary-package"
	case ".rpm":
		return "application/x-rpm"
	case ".swf":
		return "application/x-shockwave-flash"
	case ".mkv":
		return "video/x-matroska"
	case ".flv":
		return "video/x-flv"
	case ".dwg":
		return "image/vnd.dwg"
	case ".kml":
		return "application/vnd.google-earth.kml+xml"
	case ".kmz":
		return "application/vnd.google-earth.kmz"
	case ".gpx":
		return "application/gpx+xml"

	default:
		return "application/octet-stream"
	}
}
