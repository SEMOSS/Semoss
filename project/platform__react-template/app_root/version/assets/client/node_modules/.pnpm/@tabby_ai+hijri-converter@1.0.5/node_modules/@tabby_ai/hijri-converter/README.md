# hijri-converter

A Javascript package to convert accurately between Hijri and Gregorian dates using the Umm al-Qura calendar.

This project uses a Typescript interpretation of a date conversion algorithm written in Python by Mohammed H Alshehri - [hijri-converter](https://github.com/mhalshehri/hijri-converter).

Therefore, it has the same accuracy and limitations.

## Limitations

- The date range supported by converter is limited to the period from the beginning of 1343 AH (1 August 1924 CE) to the end of 1500 AH (16 November 2077 CE).
- The conversion is not intended for religious purposes where sighting of the lunar crescent at the beginning of Hijri month is still preferred.

## Comparison

| Item                      | hijri-converter    | moment-hijri                     |
|:--------------------------|:-------------------|:---------------------------------|
| Conversion range          | 1343-1500 AH       | 1356-1500 AH                     |
| Accuracy                  | 100%               | 91,8%                            |
| Input validation          | Yes                | No                               |
| Typescript support        | Typescript first   | Types declarations               |
| Functionality             | Only convert dates | Parse, manipulate, display, etc. |
| Dependencies              | Zero-dependency    | moment                           |
| Size (minified + gzipped) | 11.4 kB            | 74.4 kB                          |

## Installation

```bash
npm install @tabby.ai/hijri-converter
```

## Basic usage

```javascript
// CommonJS modules
const { gregorianToHijri } = require('@tabby.ai/hijri-converter');

const date = new Date();
const hijriDate = gregorianToHijri({
  year: date.getFullYear(),
  month: date.getMonth() + 1, // Month number in Javascript Date API is zero-based.
  day: date.getDay(),
});

console.log(hijriDate); // { year: 1444, month: 7, day: 15 }

// or ESModules
import { hijriToGregorian } from '@tabby.ai/hijri-converter';

const gregorianDate = hijriToGregorian({ year: 1444, month: 7, day: 15 });

console.log(gregorianDate); // { year: 2023, month: 2, day: 6 }
```

## What about date formatting?

This library only converts dates. But you might not need any specific date libraries if you want to format dates. Browsers and Node.js widely support date formatting: [Date.prototype.toLocaleDateString](https://caniuse.com/?search=Date.prototype.toLocaleDateString), [Intl.DateTimeFormat](https://caniuse.com/?search=DateTimeFormat)

```javascript
const date = new Date(Date.UTC(2012, 11, 20, 3, 0, 0));

const options = {
  timeZone: 'UTC',
  weekday: 'short',
  year: 'numeric',
  month: 'long',
  day: 'numeric',
};

// using Date.prototype.toLocaleDateString method
console.log(date.toLocaleDateString('ar-SA-u-ca-islamic-umalqura', options)); // -> "الخميس، ٧ صفر ١٤٣٤ هـ"
console.log(date.toLocaleDateString('en-SA-u-ca-islamic-umalqura', options)); // -> "Thu, Safar 7, 1434 AH"

// or using Intl.DateTimeFormat API
const arSaFormatter = new Intl.DateTimeFormat(
  'ar-SA-u-ca-islamic-umalqura',
  options
);
const enSaFormatter = new Intl.DateTimeFormat(
  'en-SA-u-ca-islamic-umalqura',
  options
);

console.log(arSaFormatter.format(date)); // -> 'الخميس، ٧ صفر ١٤٣٤ هـ'
console.log(enSaFormatter.format(date)); // -> "Thu, Safar 7, 1434 AH"
```

## License

This project is licensed under the terms of the MIT license.
