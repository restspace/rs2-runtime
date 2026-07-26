//! The pixel pipeline: decode (guarded), explicit crop, fit/gravity
//! geometry, resize, encode. Pure — bytes in, bytes out — so it unit-tests
//! natively without the sandbox.

use std::io::Cursor;

use image::imageops::FilterType;
use image::{DynamicImage, ImageReader};

use crate::params::{Fit, Format, Params};

pub struct Output {
    pub bytes: Vec<u8>,
    pub media_type: &'static str,
    pub width: u32,
    pub height: u32,
}

/// Transform failure, mapped to a status by the service layer.
#[derive(Debug)]
pub enum TransformError {
    /// Source exceeds the pixel guard — refused before full decode (413).
    TooLarge { pixels: u64, cap: u64 },
    /// Not a decodable image format (415).
    Unsupported(String),
    /// The parameter combination can't apply to this source (400).
    BadRequest(String),
}

/// The crop window + final dimensions implied by fit/gravity/rect against a
/// source of `sw`×`sh` (post-`rect`). `None` window means no crop.
pub fn geometry(
    sw: u32,
    sh: u32,
    p: &Params,
) -> Result<(Option<(u32, u32, u32, u32)>, (u32, u32)), TransformError> {
    let (sw_f, sh_f) = (sw as f64, sh as f64);
    match p.fit {
        Fit::Fill => {
            let (tw, th) = (p.w.unwrap(), p.h.unwrap());
            Ok((None, (tw, th)))
        }
        Fit::Cover => {
            let (tw, th) = (p.w.unwrap(), p.h.unwrap());
            let scale = (tw as f64 / sw_f).max(th as f64 / sh_f);
            // The source window that maps onto the target box.
            let cw = ((tw as f64 / scale).round() as u32).clamp(1, sw);
            let ch = ((th as f64 / scale).round() as u32).clamp(1, sh);
            let x = ((sw - cw) as f64 * p.g.x as f64).round() as u32;
            let y = ((sh - ch) as f64 * p.g.y as f64).round() as u32;
            Ok((Some((x, y, cw, ch)), (tw, th)))
        }
        Fit::Contain | Fit::ScaleDown => {
            let mut scale = f64::INFINITY;
            if let Some(tw) = p.w {
                scale = scale.min(tw as f64 / sw_f);
            }
            if let Some(th) = p.h {
                scale = scale.min(th as f64 / sh_f);
            }
            if !scale.is_finite() {
                // No box given: pure rect-crop (or format conversion).
                return Ok((None, (sw, sh)));
            }
            if p.fit == Fit::ScaleDown {
                scale = scale.min(1.0);
            }
            let tw = ((sw_f * scale).round() as u32).max(1);
            let th = ((sh_f * scale).round() as u32).max(1);
            Ok((None, (tw, th)))
        }
    }
}

/// Header-only probe: dimensions without a pixel decode (the `$info` path
/// and the decompression-bomb guard).
pub fn probe(source: &[u8]) -> Result<(u32, u32), TransformError> {
    ImageReader::new(Cursor::new(source))
        .with_guessed_format()
        .map_err(|e| TransformError::Unsupported(e.to_string()))?
        .into_dimensions()
        .map_err(|e| TransformError::Unsupported(e.to_string()))
}

/// Decode → crop → resize → encode. `resolved` must be concrete (never
/// `Auto` — the service resolves it against the source media type first).
pub fn transform(
    source: &[u8],
    p: &Params,
    resolved: Format,
    max_source_pixels: u64,
) -> Result<Output, TransformError> {
    // Dimension guard from the header alone — a decompression bomb is
    // refused before pixel decode allocates anything.
    let (dw, dh) = probe(source)?;
    let pixels = dw as u64 * dh as u64;
    if pixels > max_source_pixels {
        return Err(TransformError::TooLarge {
            pixels,
            cap: max_source_pixels,
        });
    }

    let mut img = ImageReader::new(Cursor::new(source))
        .with_guessed_format()
        .map_err(|e| TransformError::Unsupported(e.to_string()))?
        .decode()
        .map_err(|e| TransformError::Unsupported(e.to_string()))?;

    if let Some(r) = p.rect {
        if r.x >= img.width() || r.y >= img.height() {
            return Err(TransformError::BadRequest(format!(
                "rect origin ({},{}) is outside the {}x{} source",
                r.x,
                r.y,
                img.width(),
                img.height()
            )));
        }
        let w = r.w.min(img.width() - r.x);
        let h = r.h.min(img.height() - r.y);
        img = img.crop_imm(r.x, r.y, w, h);
    }

    let (window, (tw, th)) = geometry(img.width(), img.height(), p)?;
    if let Some((x, y, cw, ch)) = window {
        img = img.crop_imm(x, y, cw, ch);
    }
    if (tw, th) != (img.width(), img.height()) {
        img = img.resize_exact(tw, th, FilterType::Lanczos3);
    }

    let mut bytes = Vec::new();
    let media_type = match resolved {
        Format::Jpeg => {
            // JPEG has no alpha; drop it rather than fail.
            let rgb = DynamicImage::ImageRgb8(img.to_rgb8());
            let enc = image::codecs::jpeg::JpegEncoder::new_with_quality(
                Cursor::new(&mut bytes),
                p.quality,
            );
            rgb.write_with_encoder(enc)
                .map_err(|e| TransformError::Unsupported(e.to_string()))?;
            "image/jpeg"
        }
        Format::Png => {
            img.write_to(&mut Cursor::new(&mut bytes), image::ImageFormat::Png)
                .map_err(|e| TransformError::Unsupported(e.to_string()))?;
            "image/png"
        }
        Format::Webp => {
            // The pure-Rust encoder is lossless-only; lossy WebP is a bundle
            // upgrade when a suitable encoder lands.
            img.write_to(&mut Cursor::new(&mut bytes), image::ImageFormat::WebP)
                .map_err(|e| TransformError::Unsupported(e.to_string()))?;
            "image/webp"
        }
        Format::Auto => unreachable!("resolved before transform"),
    };
    Ok(Output {
        bytes,
        media_type,
        width: tw,
        height: th,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::{parse, Config, Format};

    /// A gradient source so resizes have real content.
    fn png_fixture(w: u32, h: u32) -> Vec<u8> {
        let img = image::RgbaImage::from_fn(w, h, |x, y| {
            image::Rgba([(x % 256) as u8, (y % 256) as u8, 128, 255])
        });
        let mut bytes = Vec::new();
        DynamicImage::ImageRgba8(img)
            .write_to(&mut Cursor::new(&mut bytes), image::ImageFormat::Png)
            .unwrap();
        bytes
    }

    fn params(query: &str) -> crate::params::Params {
        parse(query, &Config::default()).unwrap().unwrap()
    }

    #[test]
    fn scale_down_shrinks_but_never_enlarges() {
        let src = png_fixture(200, 100);
        let out = transform(&src, &params("w=100"), Format::Png, u64::MAX).unwrap();
        assert_eq!((out.width, out.height), (100, 50));
        let out = transform(&src, &params("w=800"), Format::Png, u64::MAX).unwrap();
        assert_eq!((out.width, out.height), (200, 100), "no upscale");
    }

    #[test]
    fn contain_enlarges_when_asked() {
        let src = png_fixture(200, 100);
        let out = transform(&src, &params("w=400&fit=contain"), Format::Png, u64::MAX).unwrap();
        assert_eq!((out.width, out.height), (400, 200));
    }

    #[test]
    fn cover_crops_to_the_exact_box() {
        let src = png_fixture(200, 100);
        let out = transform(&src, &params("w=50&h=50&fit=cover"), Format::Png, u64::MAX).unwrap();
        assert_eq!((out.width, out.height), (50, 50));
    }

    #[test]
    fn cover_gravity_picks_the_window() {
        // 4x1 source: pixel columns 0..4 have distinct red values; a 1x1
        // cover crop at west vs east must sample different columns.
        let img = image::RgbaImage::from_fn(4, 1, |x, _| image::Rgba([(x * 60) as u8, 0, 0, 255]));
        let mut src = Vec::new();
        DynamicImage::ImageRgba8(img)
            .write_to(&mut Cursor::new(&mut src), image::ImageFormat::Png)
            .unwrap();
        let west = transform(&src, &params("w=1&h=1&fit=cover&g=w"), Format::Png, u64::MAX).unwrap();
        let east = transform(&src, &params("w=1&h=1&fit=cover&g=e"), Format::Png, u64::MAX).unwrap();
        let wpx = image::load_from_memory(&west.bytes).unwrap().to_rgba8()[(0, 0)];
        let epx = image::load_from_memory(&east.bytes).unwrap().to_rgba8()[(0, 0)];
        assert!(wpx[0] < epx[0], "west {wpx:?} vs east {epx:?}");
    }

    #[test]
    fn rect_crops_before_resize() {
        let src = png_fixture(200, 100);
        let out = transform(&src, &params("rect=0,0,50,50"), Format::Png, u64::MAX).unwrap();
        assert_eq!((out.width, out.height), (50, 50));
        // rect out of bounds is a bad request.
        assert!(matches!(
            transform(&src, &params("rect=300,0,50,50"), Format::Png, u64::MAX),
            Err(TransformError::BadRequest(_))
        ));
    }

    #[test]
    fn pixel_guard_refuses_oversized_sources() {
        let src = png_fixture(200, 100);
        assert!(matches!(
            transform(&src, &params("w=100"), Format::Png, 10_000),
            Err(TransformError::TooLarge { .. })
        ));
    }

    #[test]
    fn jpeg_encode_drops_alpha() {
        let src = png_fixture(50, 50);
        let out = transform(&src, &params("w=25&f=jpeg"), Format::Jpeg, u64::MAX).unwrap();
        assert_eq!(out.media_type, "image/jpeg");
        assert!(image::load_from_memory(&out.bytes).is_ok());
    }

    #[test]
    fn garbage_input_is_unsupported() {
        assert!(matches!(
            transform(b"not an image", &params("w=10"), Format::Png, u64::MAX),
            Err(TransformError::Unsupported(_))
        ));
    }
}
