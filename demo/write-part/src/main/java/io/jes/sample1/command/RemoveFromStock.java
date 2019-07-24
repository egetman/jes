package io.jes.sample1.command;

import java.util.UUID;

import io.jes.Command;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class RemoveFromStock implements Command {

    private final UUID itemUuid;

}
