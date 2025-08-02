import { exec } from 'child_process';
import { JConnection } from '../src/index';

const process0 = exec('j -p 1999');
const j0 = new JConnection({ port: 1999 });


afterAll(() => {
  process0.kill()
});

test('connect to j-star without credentials', done => {
  j0.connect(err => {
    expect(err).toBe(null);
    j0.sync('sum range 10', (_, res) => {
      expect(res).toStrictEqual(45);
      done();
    });
  });
});

test('sync', done => {
  j0.sync('18', (_, res) => {
    expect(res).toBe(18);
    done();
  });
});

test('async', done => {
  j0.asyn('18', () => {
    done();
  });
});

test('lost connection while querying', done => {
  const process1 = exec('j -p 1998');
  setTimeout(() => {
    const j1 = new JConnection({ port: 1998 });
    j1.connect(err => {
      expect(err).toBe(null);
      j1.sync('exit 0', (err, _res) => {
        expect(err.message).toBe('LOST_CONNECTION');
        process1.kill();
        done();
      });
    });
  }, 10);
});
