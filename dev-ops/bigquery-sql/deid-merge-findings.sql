CREATE TEMP FUNCTION
  applyDeid(inputText STRING, findingStrs ARRAY<STRING>)
  RETURNS STRING
  LANGUAGE js AS """

  var starr_bq_udf_offset_after = function(posMap, startPos, offset){
    for (var k in posMap) {
      if ( k >= startPos ) {
        posMap[k] += offset;
      }
    }
    return posMap;
  }
  //this function is customized from google closure.  https://github.com/google/closure-library/blob/master/closure/goog/crypt/crypt.js
  //the script converts a utf8 string to a byte array with uft8 encoding.
  //modification is adding a position map that has string position entries that need to remapped to the new byte array, so that we can
  //unify both TiDE findings which based on string position, and DLP native findings which is based on byte array position
  var goog_crypt_stringToUtf8ByteArray = function(str, posMap) {
    // added to remap string position to byte array position
    var out = [], p = 0;
    for (var i = 0; i < str.length; i++) {
      var c = str.charCodeAt(i);
      if (c < 128) {
        out[p++] = c;
      } else if (c < 2048) {
        out[p++] = (c >> 6) | 192;
        out[p++] = (c & 63) | 128;
        posMap = starr_bq_udf_offset_after(posMap,i,1);
      } else if (
          ((c & 0xFC00) == 0xD800) && (i + 1) < str.length &&
          ((str.charCodeAt(i + 1) & 0xFC00) == 0xDC00)) {
        // Surrogate Pair
        c = 0x10000 + ((c & 0x03FF) << 10) + (str.charCodeAt(++i) & 0x03FF);
        out[p++] = (c >> 18) | 240;
        out[p++] = ((c >> 12) & 63) | 128;
        out[p++] = ((c >> 6) & 63) | 128;
        out[p++] = (c & 63) | 128;
        posMap = starr_bq_udf_offset_after(posMap,i,3);
      } else {
        out[p++] = (c >> 12) | 224;
        out[p++] = ((c >> 6) & 63) | 128;
        out[p++] = (c & 63) | 128;
        posMap = starr_bq_udf_offset_after(posMap,i,2);
      }
    }
    return {inputBytes:out, posMap:posMap};
  };

var sliceBytes = function (bytestr, from, to ) {
    return Utf8ArrayToStr(bytestr, from, to);
  }

//convert byte array back to UTF8 string
//https://stackoverflow.com/questions/17191945/conversion-between-utf-8-arraybuffer-and-string
function Utf8ArrayToStr(array, from, to) {
  var out, i, len, c;
  var char2, char3;

  out = "";
  len = array.length;
  i = from;
  while (i < to) {
    c = array[i++];
    switch (c >> 4)
    {
      case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
        // 0xxxxxxx
        out += String.fromCharCode(c);
        break;
      case 12: case 13:
        // 110x xxxx   10xx xxxx
        char2 = array[i++];
        out += String.fromCharCode(((c & 0x1F) << 6) | (char2 & 0x3F));
        break;
      case 14:
        // 1110 xxxx  10xx xxxx  10xx xxxx
        char2 = array[i++];
        char3 = array[i++];
        out += String.fromCharCode(((c & 0x0F) << 12) |
                                   ((char2 & 0x3F) << 6) |
                                   ((char3 & 0x3F) << 0));
        break;
    }
  }
  return out;
}

  var out =  '';

  var findingArray = [];
  var strPosMap = {};

  for (var key in findingStrs) {
    var findingStr = findingStrs[key];
    var findings = JSON.parse(findingStr);
    for (var f in findings) {
      var finding = findings[f];
      if (finding['foundBy']!='google-dlp-native') {
        strPosMap[finding['start']] = finding['start'];
        strPosMap[finding['end']] = finding['end'];
      }
      findingArray.push(finding);
    }
  }

  var inputBytesWithRemappedPos = goog_crypt_stringToUtf8ByteArray(inputText, strPosMap);

  var inputBytes = inputBytesWithRemappedPos.inputBytes;
  strPosMap = inputBytesWithRemappedPos.posMap;

  for (var key in findingArray) {
    var finding = findingArray[key];
    if (finding['foundBy']!='google-dlp-native') {
      findingArray[key]['start'] = strPosMap[finding['start']];
      findingArray[key]['end'] = strPosMap[finding['end']];
    }
  }

  findingArray.sort(function(a, b){
      var c1 = b['end'] - a['end'];
      if (c1 != 0) { return c1; }
      if (a['replacement'] == null && b['replacement'] == null) { return 0; }
      if (a['replacement'] != null && b['replacement'] != null) { return b['replacement'].length - a['replacement'].length; }
      if (a['replacement'] == null) { return 1; } else { return -1; }
    });

  if (findingArray.length == 0) {
    return inputText;
  }

  var lastEnd = Number.MAX_SAFE_INTEGER;
  var lastStart = Number.MAX_SAFE_INTEGER;
  var doneWithTheFirst = false;
  var REPLACE_WORD = '[REMOVED]';
  for (var k = 0; k < findingArray.length; k++) {
    var finding = findingArray[k];

    if (finding['start'] < 0 || finding['start'] > inputBytes.length
          || finding['end'] < 0 || finding['end'] > inputBytes.length) {
        continue;
      }

      if (!doneWithTheFirst) {
        //first instance
        out = sliceBytes(inputBytes, finding['end'], inputBytes.length ) + out;

        out = (finding['replacement'] != null
            ? finding['replacement'] :
            REPLACE_WORD) + out;

        lastEnd = finding['end'];
        lastStart = finding['start'];
        if (k == findingArray.length - 1) {
          out = sliceBytes(inputBytes,0,finding['start']) + out;
        }
        doneWithTheFirst = true;
        continue;
      }

      if (finding['end'] <= lastEnd && finding['start'] >= lastEnd) {
        //inside of last change, do nothing
      } else if (finding['end'] < lastEnd && finding['end'] >= lastStart) {
        //overlap
        lastStart = finding['start'];
      } else if (finding['end'] < lastStart) {
        //outside of last change, copy fully
        out = sliceBytes(inputBytes,finding['end'],lastStart) + out;
        out = (finding['replacement'] != null ? finding['replacement'] : REPLACE_WORD) + out;

        lastEnd = finding['end'];
        lastStart = finding['start'];
      }

      if (k == findingArray.length - 1) {
        out = sliceBytes(inputBytes,0,finding['start']) + out;
      }
  }
  return out;

"""
;


WITH
  dlp_findings AS (
  SELECT
    CAST(dlp.location.content_locations[ORDINAL(1)].record_location.record_key.id_values[ORDINAL(1)] AS INT64) AS text_id_1,
    dlp.location.content_locations[ORDINAL(1)].record_location.record_key.id_values[ORDINAL(2)] AS text_id_2,
    count(1) AS finding_cnt,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        '' AS word,
        dlp.location.byte_range.start AS `start`,
        dlp.location.byte_range.end AS `end`,
        dlp.info_type.name AS `type`,
        null AS replacement,
        'google-dlp-native' AS foundBy
        ))) AS findings
  FROM
    `[FULL_CDM_PROJECT_ID].[FULL_CDM_DATASET].[DLP_NATIVE_RESULT]` dlp
  GROUP BY
    text_id_1,
    text_id_2 )

SELECT
  tide.*,
  applyDeid(tide.note_text ,ARRAY<STRING>[FINDING_note_text, dlp_findings.findings]) as TEXT_DEID_note_text_merged,
  dlp_findings.finding_cnt as dlp_finding_cnt,
  dlp_findings.findings as dlp_findings

FROM
  `[FULL_CDM_PROJECT_ID].[FULL_CDM_DATASET].[TIDE_DEID_RESULT] tide
LEFT JOIN
  dlp_findings
ON
  dlp_findings.text_id_1 = tide.note_id
  AND dlp_findings.text_id_2 = tide.STUDY_ID
