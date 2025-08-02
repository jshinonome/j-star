import { Float64, tableFromArrays, Uint8, vectorFromArray } from 'apache-arrow';
import IPC from '../src/ipc';

function d(hexString, useBigInt = false, includeNanosecond = false) {
  return IPC.deserialize(Buffer.from(hexString, 'hex'), useBigInt, includeNanosecond);
}

function s(obj) {
  let buffers = IPC.serialize(obj);
  let header = Buffer.alloc(16);
  header.writeUInt8(0x1, 0);
  header.writeBigUInt64LE(BigInt(buffers.reduce((acc, curr) => acc + curr.length, 0)), 8);
  return Buffer.concat([header, ...buffers]).toString('hex');
}

test('deserialize/serialize general list', () => {
  const msg = '0100000000000000' +
    '4000000000000000' +
    '5a00000003000000' +
    '3000000000000000' +
    'f300000001000000' + '6a00000000000000' +
    'fb00000000000000' + '6300000000000000' +
    'f400000000000000' + '1f85eb51b81e0940';
  const obj = ['j', 99, 3.14];
  expect(d(msg)).toStrictEqual(obj);
  expect(s(obj)).toBe(msg);
});

test('deserialize/serialize function call', () => {
  const msg = '0100000000000000' +
    '6000000000000000' +
    '5a00000004000000' +
    // byte length
    '5000000000000000' +
    // function name - 40
    'f30000001a000000' + '2e63616c656e6461722e6765744461746554797065427953796d' + '000000000000' +
    // date - 16
    'f800000000000000' + '00cc4aa083010000' +
    // '7203.T' - 16
    'f300000006000000' + '373230332e540000' +
    // null - 8
    '0000000000000000';
  const obj = ['.calendar.getDateTypeBySym', new Date('2022-10-04'), '7203.T', null];
  expect(d(msg)).toStrictEqual(obj);
  expect(s(obj)).toBe(msg);
});

test('deserialize/serialize table', () => {
  const msg =
    "010000000000000038030000000000005c000000000000002803000000000000ffffffff200100001000000000000a0010000e00070008000a0000000000000108000000000004006cffffff0400000003000000bc000000500000000400000060ffffff140000000000000118000000000000031c0000000500000073636f7265000000000000000000060008000600060000000000020010001c0008000f00170018000400100010000000380000001400000000000001180000000000000518000000040000006e616d65000000000000000004000400040000000800080000000400080000000c00000008000c00080007000800000000000001200000001000180004000b001300140000000c001000000014000000000000011400000000000002180000000200000069640000000000000000060008000400060000000800000000000000ffffffffb000000014000000000000000c001c001a001300140004000c00000020000000000000000000000000000002100000000000040008000a0000000400080000001000000000000a0018000c00080004000a0000001400000048000000030000000000000000000000030000000000000000000000000000000000000000000000000000001000000000000000100000000000000010000000000000000000000001000000030000000000000000000000000000000000000005000000080000000f000000416c696365426f62436861726c696500ffffffffe800000014000000000000000c00160014000f00100004000c0000003000000000000000000000031000000004000a0018000c00080004000a0000001400000078000000030000000000000000000000060000000000000000000000000000000000000000000000000000000800000000000000080000000000000000000000000000000800000000000000100000000000000018000000000000000000000000000000180000000000000018000000000000000000000003000000030000000000000000000000000000000300000000000000000000000000000003000000000000000000000000000000010203000000000000000000010000000200000000000000000000000040554000000000002057403333333333935340ffffffff00000000";

  const table = tableFromArrays({
    id: vectorFromArray([1, 2, 3], new Uint8),
    name: ['Alice', 'Bob', 'Charlie'],
    score: vectorFromArray([85.0, 92.5, 78.3], new Float64),
  })

  expect(s(table)).toBe(msg);
  expect(d(msg).toString()).toStrictEqual(table.toString());
});

test('deserialize/serialize null', () => {
  const msg = '0100000000000000' + '0800000000000000' + '0000000000000000';
  expect(d(msg)).toBe(null);
  expect(s(null)).toBe(msg);
}
);

test('deserialize/serialize boolean true', () => {
  const msg = '0100000000000000' + '0800000000000000' + 'ff00000001000000';
  expect(d(msg)).toBe(true);
  expect(s(true)).toBe(msg);
});

test('deserialize/serialize boolean false', () => {
  const msg = '0100000000000000' + '0800000000000000' + 'ff00000000000000';
  expect(d(msg)).toBe(false);
  expect(s(false)).toBe(msg);
});


test('deserialize/serialize boolean list', () => {
  const msg = '0100000000000000' +
    '2000000000000000' +
    '5a00000002000000' +
    '1000000000000000' +
    'ff00000001000000' +
    'ff00000000000000';
  const obj = [true, false];
  expect(d(msg)).toStrictEqual(obj);
  expect(s(obj)).toBe(msg);
});

test('deserialize u8', () => {
  expect(d('0100000000000000' + '0800000000000000' + 'fe00000001000000')).toBe(1);
});

test('deserialize i16', () => {
  expect(d('0100000000000000' + '0800000000000000' + 'fd000000aa000000')).toBe(170);
});

test('deserialize i32', () => {
  expect(d('0100000000000000' + '0800000000000000' + 'fc00000063000000')).toBe(99);
});

test('deserialize/serialize i64', () => {
  let msg = '0100000000000000' + '1000000000000000' + 'fb000000000000006300000000000000';
  expect(d(msg)).toBe(99);
  expect(s(99)).toBe(msg);
  expect(d(msg, true)).toBe(99n);
  expect(s(99n)).toBe(msg);
});

test('deserialize f32', () => {
  expect(d('0100000000000000' + '0800000000000000' + 'f50000000000c642')).toBe(99);
  expect(d('0100000000000000' + '0800000000000000' + 'f50000000000c0ff')).toBe(NaN);
  expect(d('0100000000000000' + '0800000000000000' + 'f50000000000807f')).toBe(Infinity);
  expect(d('0100000000000000' + '0800000000000000' + 'f5000000000080ff')).toBe(-Infinity);
});

test('deserialize f64', () => {
  expect(d('0100000000000000' + '1000000000000000' + 'f400000000000000' + '0000000000c05840')).toBe(99);
});

// 000000000000f8ff or 000000000000f87f => NaN
// as writeDoubleLE write NaN 000000000000f87f, use 000000000000f87f here
test('deserialize/serialize f64 NaN', () => {
  const msg = '0100000000000000' + '1000000000000000' + 'f400000000000000' + '000000000000f87f';
  const obj = NaN;
  expect(d(msg)).toBe(obj);
  expect(s(obj)).toBe(msg);
});

test('deserialize/serialize f64 infinite', () => {
  const msg = '0100000000000000' + '1000000000000000' + 'f400000000000000' + '000000000000f07f';
  const obj = Infinity;
  expect(d(msg)).toBe(obj);
  expect(s(obj)).toBe(msg);
});

test('deserialize/serialize f64 -infinite', () => {
  const msg = '0100000000000000' + '1000000000000000' + 'f400000000000000' + '000000000000f0ff';
  const obj = -Infinity;
  expect(d(msg)).toBe(obj);
  expect(s(obj)).toBe(msg);
});

test('deserialize/serialize utf8 string', () => {
  const msg = '0100000000000000' + '1800000000000000' + 'f30000000c000000' + 'e38282e381aee381aee38191' + '00000000'
  const obj = 'もののけ';
  expect(d(msg)).toBe(obj);
  expect(s(obj)).toBe(msg);
});


test('deserialize symbol', () => {
  expect(d('0100000000000000' + '1000000000000000' + 'f200000001000000' + '6100000000000000')).toBe('a');
});

test('deserialize timestamp', () => {
  const header = '0100000000000000' + '1000000000000000' + 'f700000000000000'
  expect(d(header + '605fe30e6849f709')).toStrictEqual(new Date('1992-10-03T14:42:56.864Z'));
  expect(d(header + '0000000000000000')).toStrictEqual(new Date('1970-01-01T00:00:00.000Z'));
  expect(d(header + 'ffffffffffffff7f')).toStrictEqual(new Date('2262-04-11T23:47:16.854Z'));
  expect(d(header + '0100000000000000')).toStrictEqual(new Date('1970-01-01T00:00:00.000Z'));
});

test('deserialize timestamp include nanosecond', () => {
  const header = '0100000000000000' + '1000000000000000' + 'f700000000000000'
  expect(d(header + '4f13ca13115eff09', false, true)).toStrictEqual('1992-10-29T22:31:32.842033999');
  expect(d(header + '0000000000000000', false, true)).toBe('1970-01-01T00:00:00.000000000');
  expect(d(header + 'ffffffffffffff7f', false, true)).toBe('2262-04-11T23:47:16.854775807');
  expect(d(header + '0100000000000000', false, true)).toBe('1970-01-01T00:00:00.000000001');
});

test('deserialize/serialize datetime', () => {
  const msg = '0100000000000000' + '1000000000000000' + 'f800000000000000' + 'e0207d33a7000000';
  const obj = new Date('1992-10-03T14:42:56.864Z');
  expect(d(msg)).toStrictEqual(obj);
  expect(s(obj)).toBe(msg);
});

test('deserialize date', () => {
  const header = '0100000000000000' + '0800000000000000' + 'fa000000';
  expect(d(header + '6feeffff')).toStrictEqual(new Date('1957-09-09'));
  expect(d(header + '77200000')).toStrictEqual(new Date('1992-10-03'));
});

test('deserialize datetime', () => {
  const header = '0100000000000000' + '1000000000000000' + 'f800000000000000';
  expect(d(header + '95bcacf681000000')).toStrictEqual(new Date('1987-09-09T12:34:56.789Z'));
  expect(d(header + '00743b9e83010000')).toStrictEqual(new Date('2022-10-03T14:24:00.000Z'));
});

test('deserialize duration', () => {
  const header = '0100000000000000' + '1000000000000000' + 'f600000000000000';
  expect(d(header + '98abebe4ccecffff')).toStrictEqual('-0D05:51:50.218577000');
  expect(d(header + '6854141b33130000')).toStrictEqual('0D05:51:50.218577000');
});

test('deserialize time', () => {
  const header = '0100000000000000' + '1000000000000000' + 'f900000000000000';
  expect(d(header + '00119be57b2e0000')).toStrictEqual('14:11:49.668000000');
  expect(d(header + 'ffff4e91944e0000')).toStrictEqual('23:59:59.999999999');
});

test('deserialize/serialize dict', () => {
  const msg =
    '0100000000000000' +
    '5000000000000000' +
    '5b00000002000000' +
    '3000000000000000' +
    '1000000000000000' +
    '0300000008000000' +
    '73796d70726963652000000000000000' +
    'f300000006000000' +
    '383330362e540000' +
    'f400000000000000' +
    '9a99999999e18440';
  const map = new Map([
    ['sym', '8306.T'],
    ['price', 668.2],
  ]);
  expect(s(map)).toBe(msg);
  expect(d(msg)).toStrictEqual(map);
});

test('deserialize fn', () => {
  const msg = '0100000000000000' + '1800000000000000' + '9a0000000a000000' + '7b5b783b795d782b797d000000000000';
  const obj = '{[x;y]x+y}';
  expect(d(msg)).toBe(obj);
});
