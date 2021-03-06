use crate::{
    data::ShiftVec,
    view::{Install, Transition, View},
};

pub(in crate::discovery) struct Frame {
    base: usize,
    highway: Vec<Install>,
    metadata: Vec<Metadata>,
    lookup: ShiftVec<usize>,
}

#[derive(Clone)]
struct Metadata {
    source_height: usize,
    destination_height: usize,
    tailless: bool,
}

impl Frame {
    pub fn genesis(genesis: &View) -> Frame {
        Frame {
            base: genesis.height(),
            highway: Vec::new(),
            metadata: Vec::new(),
            lookup: ShiftVec::new(genesis.height()),
        }
    }

    pub fn update(&self, install: Install) -> Option<Frame> {
        let transition = install.clone().into_transition();

        if self.can_grow_by(&transition) || self.can_improve_by(&transition) {
            Some(self.acquire(install, transition))
        } else {
            None
        }
    }

    pub fn top(&self) -> usize {
        self.metadata
            .last()
            .map(|metadata| metadata.destination_height)
            .unwrap_or(self.base)
    }

    pub fn lookup(&self, height: usize) -> Vec<Install> {
        let height = height.clamp(self.base, self.top());

        if height < self.top() {
            self.highway[self.lookup[height]..].to_vec()
        } else {
            vec![]
        }
    }

    fn acquire(&self, install: Install, transition: Transition) -> Frame {
        let base = self.base;

        let mut highway = Vec::new();
        let mut metadata = Vec::new();

        if let Some(to) = self.locate_by_destination(transition.source().height()) {
            highway.extend_from_slice(&self.highway[..=to]);
            metadata.extend_from_slice(&self.metadata[..=to]);
        }

        highway.push(install);

        metadata.push(Metadata {
            source_height: transition.source().height(),
            destination_height: transition.destination().height(),
            tailless: transition.tailless(),
        });

        if let Some(from) = self.locate_by_source(transition.destination().height()) {
            highway.extend_from_slice(&self.highway[from..]);
            metadata.extend_from_slice(&self.metadata[from..]);
        }

        let mut lookup = ShiftVec::new(base);
        let mut last_tailless = 0;

        for (index, metadata) in metadata.iter().enumerate() {
            if metadata.tailless {
                while lookup.len() < metadata.destination_height {
                    lookup.push(last_tailless)
                }
                last_tailless = index + 1;
            }
        }

        let top = metadata.last().unwrap().destination_height;

        while lookup.len() < top {
            lookup.push(last_tailless);
        }

        Self {
            base,
            highway,
            metadata,
            lookup,
        }
    }

    fn can_grow_by(&self, transition: &Transition) -> bool {
        transition.destination().height() > self.top()
    }

    fn can_improve_by(&self, transition: &Transition) -> bool {
        if let (Some(source), Some(destination)) = (
            self.locate_by_source(transition.source().height()),
            self.locate_by_destination(transition.destination().height()),
        ) {
            (source < destination)
                || (transition.tailless() && !self.metadata[destination].tailless)
        } else {
            false
        }
    }

    fn locate_by_source(&self, height: usize) -> Option<usize> {
        self.metadata
            .binary_search_by_key(&height, |metadata| metadata.source_height)
            .ok()
    }

    fn locate_by_destination(&self, height: usize) -> Option<usize> {
        self.metadata
            .binary_search_by_key(&height, |metadata| metadata.destination_height)
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::view::test::{generate_installs, last_installable, Client, InstallGenerator};

    fn setup(genesis_height: usize, max_height: usize) -> (Frame, InstallGenerator) {
        let generator = InstallGenerator::new(max_height);
        let genesis = generator.view(genesis_height);
        let frame = Frame::genesis(&genesis);

        (frame, generator)
    }

    fn check_lookup(frame: &Frame, genesis_height: usize, expected: &[usize]) {
        for (index, expected) in expected.into_iter().enumerate() {
            assert_eq!(frame.lookup[genesis_height + index], *expected);
        }
    }

    fn check_frame<I>(
        frame: &Frame,
        genesis_height: usize,
        tailless: I,
        generator: &InstallGenerator,
    ) where
        I: IntoIterator<Item = usize>,
    {
        for (current, last_installable) in
            last_installable(genesis_height, generator.max_height(), tailless)
                .into_iter()
                .enumerate()
                .filter(|(height, _)| *height >= genesis_height)
        {
            let mut client = Client::new(generator.view(current), generator.view(last_installable));

            let installs = frame.lookup(current);
            client.update(installs);

            assert!(client.current().height() >= frame.top());
        }
    }

    #[test]
    fn manual() {
        const GENESIS_HEIGHT: usize = 10;
        const MAX_HEIGHT: usize = 50;

        let (frame, generator) = setup(GENESIS_HEIGHT, MAX_HEIGHT);

        let i0 = generator.install(10, 15, [16]);
        let f0 = frame.update(i0).unwrap();

        let i1 = generator.install(15, 20, [21]);
        let f1 = f0.update(i1).unwrap();

        let i2 = generator.install(20, 25, []);
        let f2 = f1.update(i2).unwrap();

        let i3 = generator.install(25, 30, [31]);
        let f3 = f2.update(i3).unwrap();

        let i4 = generator.install(30, 35, []);
        let f4 = f3.update(i4).unwrap();

        let i5 = generator.install(35, 40, []);
        let f5 = f4.update(i5).unwrap();

        let expected = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 5, 5, 5, 5,
            5,
        ];

        check_lookup(&f5, GENESIS_HEIGHT, expected);
        check_frame(&f5, GENESIS_HEIGHT, [25, 35, 40], &generator);
    }

    #[test]
    fn all_tailless() {
        const GENESIS_HEIGHT: usize = 10;
        const MAX_HEIGHT: usize = 20;

        let (mut frame, generator) = setup(GENESIS_HEIGHT, MAX_HEIGHT);

        for i in GENESIS_HEIGHT..MAX_HEIGHT {
            let install = generator.install(i, i + 1, []);
            frame = frame.update(install).unwrap();
        }

        let expected = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        check_lookup(&frame, GENESIS_HEIGHT, expected);
        check_frame(&frame, GENESIS_HEIGHT, 10..21, &generator);
    }

    #[test]
    fn no_tailless() {
        const GENESIS_HEIGHT: usize = 10;
        const MAX_HEIGHT: usize = 21;

        let (mut frame, generator) = setup(GENESIS_HEIGHT, MAX_HEIGHT);

        for i in GENESIS_HEIGHT..(MAX_HEIGHT - 1) {
            let install = generator.install(i, i + 1, [i + 2]);
            frame = frame.update(install).unwrap();
        }

        let expected = &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        check_lookup(&frame, GENESIS_HEIGHT, expected);
        check_frame(&frame, GENESIS_HEIGHT, [], &generator);
    }

    #[test]
    fn new_tailless() {
        const GENESIS_HEIGHT: usize = 10;
        const MAX_HEIGHT: usize = 21;

        let (mut frame, generator) = setup(GENESIS_HEIGHT, MAX_HEIGHT);

        for i in GENESIS_HEIGHT..(MAX_HEIGHT - 1) {
            let install = generator.install(i, i + 1, [i + 2]);
            frame = frame.update(install).unwrap();
        }

        let expected = &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        check_lookup(&frame, GENESIS_HEIGHT, expected);
        check_frame(&frame, GENESIS_HEIGHT, [], &generator);

        for i in [15, 17] {
            let install = generator.install(i - 1, i, []);
            frame = frame.update(install).unwrap();
        }

        let expected = &[0, 0, 0, 0, 0, 5, 5, 7, 7, 7];

        check_lookup(&frame, GENESIS_HEIGHT, expected);
        check_frame(&frame, GENESIS_HEIGHT, [15, 17], &generator);
    }

    #[test]
    fn shortcut_tailless() {
        const GENESIS_HEIGHT: usize = 10;
        const MAX_HEIGHT: usize = 21;

        let (mut frame, generator) = setup(GENESIS_HEIGHT, MAX_HEIGHT);

        let i0 = generator.install(10, 11, [12, 13]);
        frame = frame.update(i0).unwrap();

        let i1 = generator.install(11, 12, [13]);
        frame = frame.update(i1).unwrap();

        let i2 = generator.install(12, 13, []);
        frame = frame.update(i2).unwrap();

        let i3 = generator.install(13, 14, [15]);
        frame = frame.update(i3).unwrap();

        let expected = &[0, 0, 0, 3];

        check_lookup(&frame, GENESIS_HEIGHT, expected);
        check_frame(&frame, GENESIS_HEIGHT, [13], &generator);

        let i4 = generator.install(10, 12, []);
        frame = frame.update(i4).unwrap();

        let expected = &[0, 0, 1, 2];

        check_lookup(&frame, GENESIS_HEIGHT, expected);
        check_frame(&frame, GENESIS_HEIGHT, [12, 13], &generator);
    }

    #[test]
    fn shortcut_tails() {
        const GENESIS_HEIGHT: usize = 10;
        const MAX_HEIGHT: usize = 21;

        let (mut frame, generator) = setup(GENESIS_HEIGHT, MAX_HEIGHT);

        let i0 = generator.install(10, 11, [12, 13]);
        frame = frame.update(i0).unwrap();

        let i1 = generator.install(11, 12, [13]);
        frame = frame.update(i1).unwrap();

        let i2 = generator.install(12, 13, []);
        frame = frame.update(i2).unwrap();

        let i3 = generator.install(13, 14, [15]);
        frame = frame.update(i3).unwrap();

        let expected = &[0, 0, 0, 3];

        check_lookup(&frame, GENESIS_HEIGHT, expected);
        check_frame(&frame, GENESIS_HEIGHT, [13], &generator);

        let i4 = generator.install(10, 12, [13]);
        frame = frame.update(i4).unwrap();

        let expected = &[0, 0, 0, 2];

        check_lookup(&frame, GENESIS_HEIGHT, expected);
        check_frame(&frame, GENESIS_HEIGHT, [13], &generator);
    }

    #[test]
    fn stress_light_checks() {
        const GENESIS_HEIGHT: usize = 10;
        const MAX_HEIGHT: usize = 50; // 100 ~= 2 seconds, 500 ~= 65 seconds

        let (mut frame, generator) = setup(GENESIS_HEIGHT, MAX_HEIGHT);

        let installs =
            generate_installs(GENESIS_HEIGHT, MAX_HEIGHT, MAX_HEIGHT / 5, MAX_HEIGHT / 15);

        let mut tailless = Vec::new();

        for (source, destination, tail) in installs.into_iter() {
            if tail.len() == 0 {
                tailless.push(destination);
            }

            let install = generator.install_dummy(source, destination, tail);

            if let Some(new) = frame.update(install) {
                frame = new;
            }
        }

        assert_eq!(frame.top(), MAX_HEIGHT - 1);
        check_frame(&frame, GENESIS_HEIGHT, tailless, &generator);
    }

    #[test]
    #[ignore]
    fn stress_heavy_checks() {
        const GENESIS_HEIGHT: usize = 10;
        const MAX_HEIGHT: usize = 100; // 100 ~= 14 seconds

        let (mut frame, generator) = setup(GENESIS_HEIGHT, MAX_HEIGHT);

        let installs =
            generate_installs(GENESIS_HEIGHT, MAX_HEIGHT, MAX_HEIGHT / 5, MAX_HEIGHT / 15);

        let mut tailless = Vec::new();
        for (source, destination, tail) in installs.into_iter() {
            if tail.len() == 0 {
                tailless.push(destination);
            }

            let install = generator.install_dummy(source, destination, tail);

            if let Some(new) = frame.update(install) {
                frame = new;
                check_frame(&frame, GENESIS_HEIGHT, tailless.iter().cloned(), &generator);
            }
        }

        assert_eq!(frame.top(), MAX_HEIGHT - 1);
    }
}
