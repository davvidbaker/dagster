import * as React from "react";
import styled from "styled-components";
import { Toaster, Tag, Position, Intent } from "@blueprintjs/core";
import { IStepDisplayEvent } from "../RunMetadataProvider";
import { showCustomAlert } from "../CustomAlertProvider";

const SharedToaster = Toaster.create({ position: Position.TOP }, document.body);

interface DisplayEventProps {
  event: IStepDisplayEvent;
}

async function copyValue(event: React.MouseEvent<any>, value: string) {
  event.preventDefault();

  const el = document.createElement("input");
  document.body.appendChild(el);
  el.value = value;
  el.select();
  document.execCommand("copy");
  el.remove();

  SharedToaster.show({
    message: "Copied to clipboard!",
    icon: "clipboard",
    intent: Intent.NONE
  });
}

const DisplayEventItem: React.FunctionComponent<
  IStepDisplayEvent["items"][0]
> = ({ action, actionText, actionValue, text }) => {
  let actionEl: React.ReactNode = actionText;

  if (action === "copy") {
    actionEl = (
      <DisplayEventLink
        title={"Copy to clipboard"}
        onClick={e => copyValue(e, actionValue)}
      >
        {actionText}
      </DisplayEventLink>
    );
  }
  if (action === "open-in-tab") {
    actionEl = (
      <DisplayEventLink
        href={actionValue}
        title={`Open in a new tab`}
        target="__blank"
      >
        {actionText}
      </DisplayEventLink>
    );
  }
  if (action === "show-in-modal") {
    actionEl = (
      <DisplayEventLink
        title="Show full value"
        onClick={() =>
          showCustomAlert({ message: actionValue, pre: true, title: "Value" })
        }
      >
        {actionText}
      </DisplayEventLink>
    );
  }
  return (
    <DisplayEventItemContainer>
      {text}: {actionEl}
    </DisplayEventItemContainer>
  );
};

export const DisplayEvent: React.FunctionComponent<DisplayEventProps> = ({
  event
}) => (
  <DisplayEventContainer>
    <DisplayEventHeader>
      {"status" in event
        ? IconComponents[(event as any).icon]("Expectation")
        : IconComponents[event.icon]("Materialization")}
      {event.text}
    </DisplayEventHeader>
    {event.items.map((item, idx) => (
      <DisplayEventItem {...item} key={idx} />
    ))}
  </DisplayEventContainer>
);

const DisplayEventContainer = styled.div`
  white-space: pre-wrap;
  font-size: 12px;
`;

const DisplayEventHeader = styled.div`
  display: flex;
  align-items: baseline;
  font-weight: 500;
`;

const DisplayEventItemContainer = styled.div`
  display: block;
  padding-left: 15px;
`;

const DisplayEventLink = styled.a`
  text-decoration: underline;
  color: inherit;
  &:hover {
    color: inherit;
  }
`;

const TinyStatusDot = styled.div`
  width: 10px;
  height: 7px;
  border-radius: 3px;
  margin-right: 4px;
  flex-shrink: 0;
`;

const IconComponents: { [key: string]: (word: string) => React.ReactNode } = {
  "dot-success": (word: string) => (
    <Tag minimal={true} intent={"success"} style={{ marginRight: 4 }}>
      {word}
    </Tag>
  ),
  "dot-failure": (word: string) => (
    <Tag minimal={true} intent={"danger"} style={{ marginRight: 4 }}>
      {word}
    </Tag>
  ),
  "dot-pending": (word: string) => (
    <Tag minimal={true} intent={"none"} style={{ marginRight: 4 }}>
      {word}
    </Tag>
  ),
  none: (word: string) => (
    <Tag minimal={true} intent={"none"} style={{ marginRight: 4 }}>
      {word}
    </Tag>
  ),
  file: (word: string) => (
    <img
      style={{ flexShrink: 0, alignSelf: "center" }}
      src={require("../images/icon-file.svg")}
    />
  ),
  link: (word: string) => (
    <img
      style={{ flexShrink: 0, alignSelf: "center" }}
      src={require("../images/icon-link.svg")}
    />
  )
};
